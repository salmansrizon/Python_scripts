import sys
import gc
import re
import time
import logging
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Optional, Dict, Any, List

import trino
import urllib3
import numpy as np
import pandas as pd
from sqlalchemy import text, types as sa_types, inspect
from sqlalchemy.exc import OperationalError, DisconnectionError

# Add project root to path so utils can be imported
sys.path.append(r'C://Python_scripts')
from utils.log_file import LogManager
from utils.db_handler import create_sqlalchemy_engine_ct, create_sqlalchemy_engine_2, TRINO_CONFIG

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Fix Windows console Unicode encoding
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass  # Not available in all environments (e.g. Dagster subprocess)

# =====================================================
# CONFIGURATION
# =====================================================
class Config:
    # Source (Trino) settings — sourced from .env via TRINO_CONFIG in db_handler
    TRINO_HOST    = TRINO_CONFIG["host"]
    TRINO_PORT    = TRINO_CONFIG["port"]
    TRINO_USER    = TRINO_CONFIG["user"]
    TRINO_CATALOG = TRINO_CONFIG["catalog"]
    TRINO_SCHEMA  = TRINO_CONFIG["schema"]
    SOURCE_TABLE  = TRINO_CONFIG["source_table"]

    # INCREMENTAL LOAD:
    DAYS_HISTORY = 30

    # Target (PostgreSQL) settings — connection managed by create_sqlalchemy_engine_ct()
    TARGET_TABLE = "fe_clevertap"
    SCHEMA = "public"

    # Processing settings
    CHUNK_SIZE = 5000
    BATCH_SIZE = 1000
    WINDOW_SIZE_HOURS = 1
    
    # Connection settings
    PG_POOL_SIZE = 5
    PG_MAX_OVERFLOW = 10
    PG_POOL_TIMEOUT = 30
    PG_POOL_RECYCLE = 300
    
    TRINO_TIMEOUT = 3600
    TRINO_CONN_RETRIES = 3
    TRINO_RETRY_DELAY = 5
    
    # Retry settings
    MAX_RETRIES = 5
    RETRY_DELAY = 10
    RETRY_BACKOFF = 2

    # ID patterns
    ID_PATTERN = re.compile(r'^\d+$')
    EXTRACT_PATTERN = re.compile(r'\d+')

# =====================================================
# LOGGING SETUP  (uses project-wide LogManager)
# =====================================================
_log_manager = LogManager("CT_events")   # creates dated log file under C:/Python_scripts/log/
logger = logging.getLogger(__name__)
# Also mirror to stdout so Dagster captures output
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(_stream_handler)
logger.setLevel(logging.INFO)

# =====================================================
# TIMER
# =====================================================
class Timer:
    def __init__(self):
        self.start = time.perf_counter()
        self.last_log = self.start

    def elapsed(self):
        return time.perf_counter() - self.start

    def log(self, step):
        now = time.perf_counter()
        logger.info(f"[{step}] {now - self.last_log:.2f}s (Total: {now - self.start:.2f}s)")
        self.last_log = now

timer = Timer()

# =====================================================
# RETRY DECORATOR
# =====================================================
def retry_on_failure(max_retries=Config.MAX_RETRIES, delay=Config.RETRY_DELAY, backoff=Config.RETRY_BACKOFF):
    """Decorator to retry functions on failure"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"All {max_retries} attempts failed for {func.__name__}: {e}")
                        raise
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}")
                    logger.warning(f"Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            return None
        return wrapper
    return decorator

# =====================================================
# DATABASE SCHEMA INSPECTOR
# =====================================================
def get_table_columns(engine, table_name: str) -> List[str]:
    """Get actual column names from database table"""
    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        logger.info(f"Columns in {table_name}: {columns}")
        return columns
    except Exception as e:
        logger.warning(f"Could not inspect table columns: {e}")
        return []

def get_column_name_case_insensitive(columns: List[str], search_name: str) -> Optional[str]:
    """Find actual column name ignoring case"""
    search_lower = search_name.lower()
    for col in columns:
        if col.lower() == search_lower:
            return col
    return None

# =====================================================
# INCREMENTAL LOAD HELPERS
# =====================================================
def table_exists(engine, table_name: str, schema: str) -> bool:
    """Check if target table exists in PostgreSQL"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_name = :table
                )
            """), {"schema": schema, "table": table_name})
            return result.scalar()
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        return False

def get_latest_timestamp_in_pg(engine) -> Optional[datetime]:
    """Get MAX(event_timestamp) already loaded in PostgreSQL"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT MAX(event_timestamp)
                FROM {Config.SCHEMA}."{Config.TARGET_TABLE}"
            """))
            val = result.scalar()
            if val is not None:
                if not isinstance(val, datetime):
                    val = pd.to_datetime(val).to_pydatetime()
                logger.info(f"Latest event_timestamp in PostgreSQL: {val}")
                return val
    except Exception as e:
        logger.warning(f"Could not query latest timestamp: {e}")
    return None

@retry_on_failure(max_retries=Config.TRINO_CONN_RETRIES, delay=Config.TRINO_RETRY_DELAY)
def get_latest_timestamp_in_trino(trino_engine) -> Optional[datetime]:
    """Get MAX(event_timestamp) available in Trino"""
    query = f"""
    SELECT MAX(TRY_CAST(event_timestamp AS TIMESTAMP))
    FROM {Config.SOURCE_TABLE}
    WHERE event_timestamp IS NOT NULL
    """
    df = pd.read_sql(query, trino_engine)
    if not df.empty and df.iloc[0, 0] is not None:
        val = pd.to_datetime(df.iloc[0, 0])
        logger.info(f"Latest event_timestamp in Trino: {val}")
        return val.to_pydatetime()
    return None

# =====================================================
# OPTIMIZED ID CLEANING
# =====================================================
@lru_cache(maxsize=10000)
def extract_numeric_id_cached(id_str: str) -> Optional[int]:
    """Cached ID extraction for repeated values"""
    if not id_str or pd.isna(id_str):
        return None
    str_value = str(id_str).strip()
    if not str_value or str_value.lower() in ['null', 'none', '']:
        return None
    if Config.ID_PATTERN.match(str_value):
        try:
            return int(str_value)
        except (ValueError, TypeError):
            pass
    match = Config.EXTRACT_PATTERN.search(str_value)
    if match:
        try:
            return int(match.group())
        except (ValueError, TypeError):
            return None
    return None

def vectorized_extract_ids(series: pd.Series) -> pd.Series:
    """Vectorized ID extraction for pandas Series"""
    str_series = series.astype(str).str.strip()
    str_series = str_series.replace(['nan', 'None', 'null'], np.nan)
    mask = str_series.notna()
    if mask.any():
        if str_series[mask].str.match(r'^\d+$').all():
            return pd.to_numeric(str_series, errors='coerce')
    extracted = str_series.str.extract(r'(\d+)', expand=False)
    return pd.to_numeric(extracted, errors='coerce')

# =====================================================
# OPTIMIZED ALLSKU LOADER
# =====================================================
def load_allsku_lookups(engine) -> Dict[str, Any]:
    """Load and prepare mv_allsku as optimized lookup structures"""
    logger.info("Loading mv_allsku table...")

    columns = get_table_columns(engine, "mv_allsku")
    
    col_map = {}
    for search_name in ['ProductId', 'VariantId', 'Vertical', 'cat1', 'deepestcategory',
                        'ShopName', 'SellerCode', 'cartup_link']:
        actual_name = get_column_name_case_insensitive(columns, search_name)
        if actual_name:
            col_map[search_name] = actual_name
            logger.info(f"Mapped {search_name} -> {actual_name}")
        else:
            col_map[search_name] = search_name
            logger.warning(f"Could not find {search_name}, using as-is")

    query = f"""
    SELECT
        "{col_map['ProductId']}"::text as product_id,
        "{col_map['VariantId']}"::text as variant_id,
        "{col_map['Vertical']}" as vertical,
        "{col_map['cat1']}" as cat1,
        "{col_map['deepestcategory']}" as deepestcategory,
        "{col_map['ShopName']}" as shopname,
        "{col_map['SellerCode']}" as sellercode,
        "{col_map['cartup_link']}" as cartup_link
    FROM mv_allsku
    WHERE "{col_map['ProductId']}" IS NOT NULL OR "{col_map['VariantId']}" IS NOT NULL
    """

    logger.info(f"Executing query...")
    
    try:
        df = pd.read_sql(query, engine)
        logger.info(f"Loaded {len(df)} rows from mv_allsku")
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return {'product_lookup': {}, 'variant_lookup': {}, 'variant_to_product': {}}

    logger.info("Cleaning allsku IDs...")
    df['clean_product_id'] = vectorized_extract_ids(df['product_id'])
    df['clean_variant_id'] = vectorized_extract_ids(df['variant_id'])
    df = df.dropna(subset=['clean_product_id', 'clean_variant_id'], how='all')
    logger.info(f"After cleaning: {len(df)} rows with valid IDs")

    if df.empty:
        logger.error("No valid IDs found in mv_allsku after cleaning!")
        return {'product_lookup': {}, 'variant_lookup': {}, 'variant_to_product': {}}

    logger.info("Building lookup dictionaries...")

    product_df = df.dropna(subset=['clean_product_id']).drop_duplicates('clean_product_id')
    product_lookup = {}
    for _, row in product_df.iterrows():
        product_lookup[row['clean_product_id']] = {
            'vertical': row.get('vertical'),
            'cat1': row.get('cat1'),
            'deepest': row.get('deepestcategory'),
            'shop': row.get('shopname'),
            'seller': row.get('sellercode'),
            'link': row.get('cartup_link')
        }

    variant_df = df.dropna(subset=['clean_variant_id']).drop_duplicates('clean_variant_id')
    variant_lookup = {}
    variant_to_product = {}
    for _, row in variant_df.iterrows():
        variant_lookup[row['clean_variant_id']] = {
            'product_id': row.get('clean_product_id'),
            'vertical': row.get('vertical'),
            'cat1': row.get('cat1'),
            'deepest': row.get('deepestcategory'),
            'shop': row.get('shopname'),
            'seller': row.get('sellercode'),
            'link': row.get('cartup_link')
        }
        if pd.notna(row.get('clean_product_id')):
            variant_to_product[row['clean_variant_id']] = row['clean_product_id']

    logger.info(f"Lookups built: {len(product_lookup)} products, {len(variant_lookup)} variants")

    del df, product_df, variant_df
    gc.collect()

    return {
        'product_lookup': product_lookup,
        'variant_lookup': variant_lookup,
        'variant_to_product': variant_to_product
    }

# =====================================================
# OPTIMIZED CHUNK PROCESSOR
# =====================================================
class ChunkProcessor:
    def __init__(self, lookups: Dict[str, Any]):
        self.product_lookup = lookups.get('product_lookup', {})
        self.variant_lookup = lookups.get('variant_lookup', {})
        self.variant_to_product = lookups.get('variant_to_product', {})
        self.enrich_cols = ['vertical', 'cat1', 'deepest_category', 'shop_name', 'seller_code', 'cartup_link']
        self.total_rows = 0
        self.enriched_rows = 0

    def process(self, chunk_df: pd.DataFrame, chunk_num: int) -> pd.DataFrame:
        """Process a single chunk"""
        original_len = len(chunk_df)
        self.total_rows += original_len

        chunk_df['event_timestamp'] = pd.to_datetime(chunk_df['event_timestamp'], errors='coerce')
        chunk_df = chunk_df.dropna(subset=['event_timestamp'])

        if 'event_date' in chunk_df.columns:
            chunk_df['event_date'] = pd.to_datetime(chunk_df['event_date'], errors='coerce').dt.date
        else:
            chunk_df['event_date'] = chunk_df['event_timestamp'].dt.date

        if chunk_df.empty:
            return pd.DataFrame()

        chunk_df['clean_product_id'] = vectorized_extract_ids(chunk_df['product_id'])
        chunk_df['clean_variant_id'] = vectorized_extract_ids(chunk_df['product_variant_id'])
        chunk_df['user_id'] = pd.to_numeric(chunk_df['user_id'], errors='coerce').fillna(0).astype(np.int64)

        if self.variant_to_product:
            missing_product_mask = chunk_df['clean_product_id'].isna() & chunk_df['clean_variant_id'].notna()
            if missing_product_mask.any():
                chunk_df.loc[missing_product_mask, 'clean_product_id'] = (
                    chunk_df.loc[missing_product_mask, 'clean_variant_id'].map(self.variant_to_product)
                )

        for col in self.enrich_cols:
            chunk_df[col] = None

        enriched_count = 0
        if self.product_lookup:
            product_mask = chunk_df['clean_product_id'].notna()
            if product_mask.any():
                for idx in chunk_df[product_mask].index:
                    pid = chunk_df.loc[idx, 'clean_product_id']
                    if pid in self.product_lookup:
                        data = self.product_lookup[pid]
                        chunk_df.loc[idx, 'vertical'] = data['vertical']
                        chunk_df.loc[idx, 'cat1'] = data['cat1']
                        chunk_df.loc[idx, 'deepest_category'] = data['deepest']
                        chunk_df.loc[idx, 'shop_name'] = data['shop']
                        chunk_df.loc[idx, 'seller_code'] = data['seller']
                        chunk_df.loc[idx, 'cartup_link'] = data['link']
                        enriched_count += 1

        if self.variant_lookup:
            remaining_mask = chunk_df['vertical'].isna() & chunk_df['clean_variant_id'].notna()
            if remaining_mask.any():
                for idx in chunk_df[remaining_mask].index:
                    vid = chunk_df.loc[idx, 'clean_variant_id']
                    if vid in self.variant_lookup:
                        data = self.variant_lookup[vid]
                        chunk_df.loc[idx, 'vertical'] = data['vertical']
                        chunk_df.loc[idx, 'cat1'] = data['cat1']
                        chunk_df.loc[idx, 'deepest_category'] = data['deepest']
                        chunk_df.loc[idx, 'shop_name'] = data['shop']
                        chunk_df.loc[idx, 'seller_code'] = data['seller']
                        chunk_df.loc[idx, 'cartup_link'] = data['link']
                        enriched_count += 1

        self.enriched_rows += enriched_count

        final_df = pd.DataFrame({
            'event_name':       chunk_df['event_name'].astype(str).str.strip(),
            'user_id':          chunk_df['user_id'],
            'product_id':       chunk_df['clean_product_id'].astype('Int64'),
            'variant_id':       chunk_df['clean_variant_id'].astype('Int64'),
            'event_date':       chunk_df['event_date'],
            'event_timestamp':  chunk_df['event_timestamp'],
            'vertical':         chunk_df['vertical'],
            'category_level1':  chunk_df['cat1'],
            'deepest_category': chunk_df['deepest_category'],
            'shop_name':        chunk_df['shop_name'],
            'seller_code':      chunk_df['seller_code'],
            'cartup_link':      chunk_df['cartup_link']
        })

        final_df = final_df.dropna(subset=['product_id', 'variant_id'], how='all')

        logger.debug(
            f"Chunk {chunk_num}: {original_len} -> {len(final_df)} rows "
            f"({len(final_df)/original_len*100:.1f}%) | Enriched: {enriched_count}"
        )

        return final_df

# =====================================================
# CREATE TARGET TABLE
# =====================================================
def create_target_table(engine):
    """Create target table on first run"""
    dtype_mapping = {
        'event_name':       sa_types.String(255),
        'user_id':          sa_types.BigInteger,
        'product_id':       sa_types.BigInteger,
        'variant_id':       sa_types.BigInteger,
        'event_date':       sa_types.Date,
        'event_timestamp':  sa_types.TIMESTAMP,
        'vertical':         sa_types.String(100),
        'category_level1':  sa_types.String(255),
        'deepest_category': sa_types.String(255),
        'shop_name':        sa_types.String(255),
        'seller_code':      sa_types.String(100),
        'cartup_link':      sa_types.String(500)
    }

    empty_df = pd.DataFrame(columns=list(dtype_mapping.keys()))
    empty_df.to_sql(
        name=Config.TARGET_TABLE,
        con=engine,
        schema=Config.SCHEMA,
        if_exists="fail",
        index=False,
        dtype=dtype_mapping
    )
    logger.info(f"Table {Config.TARGET_TABLE} created")

# =====================================================
# CREATE INDEXES
# =====================================================
def create_indexes(engine):
    """Build all indexes safely"""
    t = Config.TARGET_TABLE
    s = Config.SCHEMA

    logger.info("Ensuring indexes exist...")
    index_start = time.perf_counter()

    indexes = [
        (f"idx_{t}_timestamp",
         f'CREATE INDEX IF NOT EXISTS idx_{t}_timestamp ON {s}."{t}" (event_timestamp)',
         "B-Tree on event_timestamp"),
        (f"idx_{t}_date_brin",
         f'CREATE INDEX IF NOT EXISTS idx_{t}_date_brin ON {s}."{t}" USING BRIN (event_date)',
         "BRIN on event_date"),
        (f"idx_{t}_product",
         f'CREATE INDEX IF NOT EXISTS idx_{t}_product ON {s}."{t}" (product_id)',
         "B-Tree on product_id"),
        (f"idx_{t}_variant",
         f'CREATE INDEX IF NOT EXISTS idx_{t}_variant ON {s}."{t}" (variant_id)',
         "B-Tree on variant_id"),
        (f"idx_{t}_user",
         f'CREATE INDEX IF NOT EXISTS idx_{t}_user ON {s}."{t}" (user_id)',
         "B-Tree on user_id"),
    ]

    with engine.connect() as conn:
        for idx_name, ddl, description in indexes:
            try:
                idx_t = time.perf_counter()
                conn.execute(text(ddl))
                conn.commit()
                elapsed = time.perf_counter() - idx_t
                logger.info(f"  Index ready ({elapsed:.1f}s): {description}")
            except Exception as e:
                logger.error(f"  Failed to create index {idx_name}: {e}")

        logger.info("Running ANALYZE...")
        conn.execute(text(f'ANALYZE {s}."{t}"'))
        conn.commit()

    total_idx_time = time.perf_counter() - index_start
    logger.info(f"Index check complete in {total_idx_time:.1f}s")

# =====================================================
# BATCH INSERTER WITH RETRY AND CONNECTION RECOVERY
# =====================================================
@retry_on_failure(max_retries=3, delay=5)
def execute_batch_insert(batch, engine):
    """Execute batch insert with retry logic"""
    batch.to_sql(
        name=Config.TARGET_TABLE,
        con=engine,
        schema=Config.SCHEMA,
        if_exists="append",
        index=False,
        method='multi'
    )
    return len(batch)

def batch_insert(df: pd.DataFrame, engine, chunk_num: int) -> int:
    """Insert data in optimized batches with connection recovery"""
    if df.empty:
        return 0

    total_rows = len(df)
    inserted = 0
    failed_rows = []

    for start in range(0, total_rows, Config.BATCH_SIZE):
        end = min(start + Config.BATCH_SIZE, total_rows)
        batch = df.iloc[start:end]
        
        try:
            inserted += execute_batch_insert(batch, engine)
        except Exception as e:
            logger.error(f"Batch insert failed after retries: {e}")
            for idx, row in batch.iterrows():
                try:
                    row_df = pd.DataFrame([row])
                    for retry in range(3):
                        try:
                            row_df.to_sql(
                                name=Config.TARGET_TABLE,
                                con=engine,
                                schema=Config.SCHEMA,
                                if_exists="append",
                                index=False
                            )
                            inserted += 1
                            break
                        except Exception as row_e:
                            if retry == 2:
                                logger.error(f"Failed to insert row after 3 attempts: {row_e}")
                                failed_rows.append(idx)
                            else:
                                time.sleep(2 ** retry)
                except Exception as row_e:
                    logger.error(f"Row insert failed: {row_e}")
                    failed_rows.append(idx)

        if start % (Config.BATCH_SIZE * 5) == 0 and start > 0:
            logger.debug(f"Chunk {chunk_num}: Inserted {inserted}/{total_rows} rows")

    if failed_rows:
        logger.warning(f"Failed to insert {len(failed_rows)} rows in chunk {chunk_num}")

    return inserted

# =====================================================
# =====================================================
# DAGSTER ASSETS (NATIVE PIPELINE)
# =====================================================
try:
    from dagster import asset, AssetExecutionContext, MetadataValue
except ImportError:
    # Fallback dummies for standalone execution
    def asset(*args, **kwargs):
        def decorator(f):
            return f
        return decorator
    class AssetExecutionContext:
        @property
        def log(self): return logger
        def add_output_metadata(self, md): pass
    class MetadataValue:
        @staticmethod
        def int(val): return val
        @staticmethod
        def float(val): return val
        @staticmethod
        def text(val): return val


@asset(
    group_name="clevertap",
    description="Determines incremental load boundaries for CT Events ETL.",
    tags={"domain": "clevertap", "type": "incremental-etl"}
)
def ct_watermarks(context: AssetExecutionContext) -> Dict[str, Any]:
    context.log.info("=" * 70)
    context.log.info("INCREMENTAL ETL PIPELINE STARTING (Phase 1: Watermarks)")
    context.log.info("=" * 70)

    pg_engine = create_sqlalchemy_engine_2(logger)
    trino_engine = create_sqlalchemy_engine_ct(logger)

    try:
        is_first_run = not table_exists(pg_engine, Config.TARGET_TABLE, Config.SCHEMA)
        
        if is_first_run:
            context.log.info("Target table does not exist — FIRST RUN.")
            context.log.info(f"Will load last {Config.DAYS_HISTORY} days from Trino.")
            create_target_table(pg_engine)
            watermark_dt = datetime.now() - timedelta(days=Config.DAYS_HISTORY)
        else:
            latest_pg = get_latest_timestamp_in_pg(pg_engine)
            if latest_pg is None:
                context.log.info("Target table is empty — loading full history.")
                watermark_dt = datetime.now() - timedelta(days=Config.DAYS_HISTORY)
            else:
                watermark_dt = latest_pg
                context.log.info(f"INCREMENTAL RUN — fetching event_timestamp > {watermark_dt}")

        trino_max = get_latest_timestamp_in_trino(trino_engine)
        
        if trino_max and trino_max <= watermark_dt:
            context.log.info(
                f"No new data in Trino (latest: {trino_max}, watermark: {watermark_dt}). "
                f"Nothing to load."
            )
            return {
                "skip": True,
                "watermark_dt": watermark_dt,
                "trino_max": trino_max,
                "is_first_run": is_first_run
            }

        context.add_output_metadata({
            "watermark_dt": MetadataValue.text(watermark_dt.strftime('%Y-%m-%d %H:%M:%S')),
            "trino_max": MetadataValue.text(trino_max.strftime('%Y-%m-%d %H:%M:%S') if trino_max else "None"),
            "is_first_run": MetadataValue.text(str(is_first_run))
        })
        
        return {
            "skip": False,
            "watermark_dt": watermark_dt,
            "trino_max": trino_max,
            "is_first_run": is_first_run
        }
        
    finally:
        trino_engine.dispose()
        pg_engine.dispose()


@asset(
    group_name="clevertap",
    description="Builds in-memory lookup dictionaries from mv_allsku.",
    tags={"domain": "clevertap"}
)
def ct_lookups(context: AssetExecutionContext) -> Dict[str, Any]:
    context.log.info("Loading mv_allsku lookups... (Phase 2: Lookups)")
    pg_engine = create_sqlalchemy_engine_2(logger)
    
    try:
        lookups = load_allsku_lookups(pg_engine)
        
        if not lookups['product_lookup'] and not lookups['variant_lookup']:
            context.log.warning("No lookup data available! Check mv_allsku table.")
            
        context.add_output_metadata({
            "products_cached": MetadataValue.int(len(lookups.get('product_lookup', {}))),
            "variants_cached": MetadataValue.int(len(lookups.get('variant_lookup', {})))
        })
        
        return lookups
    finally:
        pg_engine.dispose()


@asset(
    group_name="clevertap",
    description=(
        "🔵 CleverTap Events ETL — Incremental load of user event data from Trino → PostgreSQL.\n\n"
        "Fetches events in 1-hour windows, enriches with catalog data, and bulk inserts."
    ),
    tags={"domain": "clevertap", "type": "incremental-etl", "target_table": "public.fe_clevertap"}
)
def fe_clevertap(
    context: AssetExecutionContext, 
    ct_watermarks: Dict[str, Any], 
    ct_lookups: Dict[str, Any]
) -> int:
    if ct_watermarks.get("skip"):
        context.log.info("Skipping data load Phase 3: No new data available.")
        return 0

    watermark_dt = ct_watermarks["watermark_dt"]
    trino_max = ct_watermarks["trino_max"]
    is_first_run = ct_watermarks["is_first_run"]

    processor = ChunkProcessor(ct_lookups)
    total_processed = 0
    chunk_num = 0
    start_time = time.perf_counter()

    window_start = watermark_dt
    window_size = timedelta(hours=Config.WINDOW_SIZE_HOURS)
    
    pg_engine = create_sqlalchemy_engine_2(logger)
    trino_engine = create_sqlalchemy_engine_ct(logger)

    try:
        while window_start < trino_max:
            window_end = min(window_start + window_size, trino_max)

            start_str = window_start.strftime('%Y-%m-%d %H:%M:%S')
            end_str = window_end.strftime('%Y-%m-%d %H:%M:%S')

            context.log.info(f"Processing window: {start_str} → {end_str}")

            query = f"""
            SELECT
                event_name, user_id, product_id, product_variant_id, event_date, event_timestamp
            FROM {Config.SOURCE_TABLE}
            WHERE (product_id IS NOT NULL OR product_variant_id IS NOT NULL)
                AND TRY_CAST(event_timestamp AS TIMESTAMP) > TIMESTAMP '{start_str}'
                AND TRY_CAST(event_timestamp AS TIMESTAMP) <= TIMESTAMP '{end_str}'
            ORDER BY event_timestamp ASC
            """

            window_success = False
            window_retries = 0
            max_window_retries = 3

            while not window_success and window_retries < max_window_retries:
                try:
                    for chunk in pd.read_sql(query, trino_engine, chunksize=Config.CHUNK_SIZE):
                        chunk_num += 1
                        processed_df = processor.process(chunk, chunk_num)

                        if not processed_df.empty:
                            inserted = batch_insert(processed_df, pg_engine, chunk_num)
                            total_processed += inserted

                    window_success = True
                    window_start = window_end

                except Exception as e:
                    window_retries += 1
                    context.log.error(f"Error in window {start_str} → {end_str} (attempt {window_retries}/{max_window_retries}): {e}")

                    if window_retries < max_window_retries:
                        wait_time = 30 * window_retries
                        context.log.info(f"Waiting {wait_time} seconds before retrying window...")
                        time.sleep(wait_time)

                        context.log.info("Testing database connections...")
                        try:
                            with trino_engine.connect() as conn:
                                conn.execute(text("SELECT 1"))
                            with pg_engine.connect() as conn:
                                conn.execute(text("SELECT 1"))
                        except Exception as reconnect_error:
                            context.log.error(f"Reconnection test failed: {reconnect_error}")
                    else:
                        context.log.error(f"All retries failed for window {start_str} → {end_str}")
                        raise

            gc.collect()

        total_time = time.perf_counter() - start_time
        avg_rate = total_processed / total_time if total_time > 0 else 0

        context.log.info("=" * 70)
        context.log.info("ETL LOAD COMPLETE (Phase 3)")
        context.log.info("=" * 70)
        context.log.info(f"Total rows inserted: {total_processed:,}")
        context.log.info(f"Rows enriched:       {processor.enriched_rows:,} ({processor.enriched_rows / max(total_processed, 1) * 100:.1f}%)")
        context.log.info(f"Total time:          {total_time:.2f} seconds")
        context.log.info(f"Average rate:        {avg_rate:.0f} rows/second")

        context.add_output_metadata({
            "rows_inserted": MetadataValue.int(total_processed),
            "rows_enriched": MetadataValue.int(processor.enriched_rows),
            "enrichment_pct": MetadataValue.float(processor.enriched_rows / max(total_processed, 1) * 100),
            "processing_seconds": MetadataValue.float(total_time),
            "rows_per_second": MetadataValue.float(avg_rate)
        })

        return total_processed

    finally:
        trino_engine.dispose()
        pg_engine.dispose()


@asset(
    group_name="clevertap",
    description="Rebuilds B-Tree and BRIN indexes after CT Events data load.",
    tags={"domain": "clevertap"}
)
def fe_clevertap_indexes(context: AssetExecutionContext, fe_clevertap: int) -> None:
    if fe_clevertap == 0:
        context.log.info("Skipping Indexing Phase 4: No new rows were inserted.")
        return
        
    context.log.info(f"Ensuring indexes on {Config.SCHEMA}.{Config.TARGET_TABLE}... (Phase 4)")
    pg_engine = create_sqlalchemy_engine_2(logger)
    
    try:
        create_indexes(pg_engine)
        
        with pg_engine.connect() as conn:
            count = conn.execute(
                text(f'SELECT COUNT(*) FROM {Config.SCHEMA}."{Config.TARGET_TABLE}"')
            ).scalar()
            
            new_watermark = conn.execute(
                text(f'SELECT MAX(event_timestamp) FROM {Config.SCHEMA}."{Config.TARGET_TABLE}"')
            ).scalar()
            
            context.log.info(f"Final table row count: {count:,}")
            context.log.info(f"New table watermark: {new_watermark}")
            
            context.add_output_metadata({
                "final_table_count": MetadataValue.int(count),
                "new_watermark": MetadataValue.text(str(new_watermark))
            })
            
    finally:
        pg_engine.dispose()


# =====================================================
# ENTRY POINT (Standalone Script Mode)
# =====================================================
if __name__ == "__main__":
    try:
        from dagster import build_asset_context
        
        # Build an actual Dagster context for standalone function invocation
        context = build_asset_context()
        
        w_state = ct_watermarks(context)
        lkp = ct_lookups(context)
        inserted = fe_clevertap(context, w_state, lkp)
        fe_clevertap_indexes(context, inserted)
        
        logger.info(f"ETL standalone run completed successfully - {inserted:,} rows loaded")
        
    except KeyboardInterrupt:
        logger.warning("ETL interrupted by user")
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        sys.exit(1)