from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import traceback

load_dotenv()

DB_CONFIG_1 = {
    "dbname": os.getenv("DB_NAME_1"),
    "user": os.getenv("DB_USER_1"),
    "password": os.getenv("DB_PASSWORD_1"),
    "host": os.getenv("DB_HOST_1"),
    "port": os.getenv("DB_PORT_1"),
}

DB_CONFIG_2 = {
    "dbname": os.getenv("DB_NAME_2"),
    "user": os.getenv("DB_USER_2"),
    "password": os.getenv("DB_PASSWORD_2"),
    "host": os.getenv("DB_HOST_2"),
    "port": os.getenv("DB_PORT_2"),
}


# CT Events — source Trino
TRINO_CONFIG = {
    "host": os.getenv("TRINO_HOST", "adssengine.technonext.com"),
    "port": int(os.getenv("TRINO_PORT", "443")),
    "user": os.getenv("TRINO_USER", "cartup"),
    "catalog": os.getenv("TRINO_CATALOG", "hive"),
    "schema": os.getenv("TRINO_SCHEMA", "gold_layer_cle_adj"),
    "source_table": os.getenv("TRINO_SOURCE_TABLE", "user_event_log_jan_2026_main_5"),
}

def create_sqlalchemy_engine_1(logger=None):
    """Creates a SQLAlchemy engine for the TechnoNext database."""
    try:
        if logger:
            logger.info("Creating SQLAlchemy engine for TechnoNext database")
        
        connection_string = f"postgresql://{DB_CONFIG_1['user']}:{DB_CONFIG_1['password']}@{DB_CONFIG_1['host']}:{DB_CONFIG_1['port']}/{DB_CONFIG_1['dbname']}"
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={
                'options': '-c statement_timeout=7200000', # 7,200,000ms = 2 hours
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 10,
                'keepalives_count': 5
            })
        
        # Test connection
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
        
        if logger:
            logger.info("Successfully created and tested SQLAlchemy engine for source database")
        return engine
        
    except Exception as e:
        error_msg = f"Failed to create SQLAlchemy engine for source database: {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)

def create_sqlalchemy_engine_2(logger=None):
    """Creates a SQLAlchemy engine for the PostgreSQL BI database."""
    try:
        if logger:
            logger.info("Creating SQLAlchemy engine for PostgreSQL BI database")
        
        connection_string = f"postgresql://{DB_CONFIG_2['user']}:{DB_CONFIG_2['password']}@{DB_CONFIG_2['host']}:{DB_CONFIG_2['port']}/{DB_CONFIG_2['dbname']}"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
        
        if logger:
            logger.info("Successfully created and tested SQLAlchemy engine for target database")
        return engine
        
    except Exception as e:
        error_msg = f"Failed to create SQLAlchemy engine for target database: {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)

def create_sqlalchemy_engine_ct(logger=None):
    """Creates a SQLAlchemy engine for the Trino source database (CT Events).

    Uses the ``trino`` SQLAlchemy dialect (pip install trino[sqlalchemy]).
    Connects over HTTPS with SSL verification disabled (self-signed cert).
    All credentials are sourced from TRINO_CONFIG (env vars via .env).
    """
    try:
        if logger:
            logger.info(
                f"Creating SQLAlchemy engine for Trino — "
                f"{TRINO_CONFIG['host']}:{TRINO_CONFIG['port']} / "
                f"{TRINO_CONFIG['catalog']}.{TRINO_CONFIG['schema']}"
            )

        # trino://user@host:port/catalog/schema
        connection_string = (
            f"trino://{TRINO_CONFIG['user']}"
            f"@{TRINO_CONFIG['host']}:{TRINO_CONFIG['port']}"
            f"/{TRINO_CONFIG['catalog']}/{TRINO_CONFIG['schema']}"
        )

        engine = create_engine(
            connection_string,
            connect_args={
                "http_scheme": "https",     # use HTTPS (port 443)
                "verify": False,            # disable SSL cert check (self-signed)
                "request_timeout": 3600,    # 1-hour query timeout
            },
        )

        # Test connection
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))

        if logger:
            logger.info("Successfully created and tested SQLAlchemy engine for Trino (CT Events source)")
        return engine

    except Exception as e:
        error_msg = f"Failed to create SQLAlchemy engine for Trino (CT Events source): {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)


def execute_sql_with_sqlalchemy(engine, query: str, logger=None) -> pd.DataFrame:
    """Executes a SQL query using SQLAlchemy and returns the result as a DataFrame."""
    try:
        # Check if the query is a file path
        if os.path.exists(query):
            if logger:
                logger.info(f"Reading SQL query from file: {query}")
            with open(query, 'r', encoding='utf-8') as file:
                sql_query = file.read()
        else:
            if logger:
                logger.info("Executing SQL query from string")
            sql_query = query
            
        if logger:
            logger.debug(f"SQL Query preview: {sql_query[:200]}...")
            
        with engine.connect() as conn:
            if logger:
                logger.info("Executing SQL query on database")
            result = conn.execute(text(sql_query))
            columns = result.keys()
            data = result.fetchall()
            df = pd.DataFrame(data, columns=columns)
            
            if logger:
                logger.info(f"Successfully executed query and retrieved {len(df)} records")
                logger.info(f"DataFrame columns: {list(df.columns)}")
                logger.info(f"DataFrame shape: {df.shape}")
            return df
            
    except Exception as e:
        error_msg = f"Error executing SQL query: {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)

def truncate_table_safe(engine, table_name: str, logger=None) -> None:
    """Safely truncate table using engine to avoid transaction conflicts."""
    try:
        if logger:
            logger.info(f"Starting safe truncation process for table: {table_name}")
        
        # Verify table exists first
        table_exists = verify_table_exists(engine, table_name, logger)
        if not table_exists:
            raise Exception(f"Table '{table_name}' does not exist in the database")
        
        with engine.begin() as conn:
            # Check for existing locks before truncation
            has_locks = check_table_locks(conn, table_name, logger)
            if has_locks and logger:
                logger.warning(f"Active locks detected on table '{table_name}', proceeding with caution")
            
            # Method 1: Try TRUNCATE command (fastest)
            try:
                if logger:
                    logger.info(f"Attempting TRUNCATE operation on table: {table_name}")
                
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
                if logger:
                    logger.info(f"Successfully truncated table '{table_name}' using TRUNCATE command")
                return
                
            except Exception as truncate_error:
                if logger:
                    logger.warning(f"TRUNCATE failed for table '{table_name}': {truncate_error}")
                
                # Method 2: Fallback to DELETE (slower but more compatible)
                if logger:
                    logger.info(f"Attempting DELETE operation on table: {table_name}")
                
                try:
                    result = conn.execute(text(f"DELETE FROM {table_name}"))
                    rows_deleted = result.rowcount if hasattr(result, 'rowcount') else 0
                    if logger:
                        logger.info(f"Successfully deleted {rows_deleted} rows from table '{table_name}'")
                    
                    # Try to reset auto-increment sequence if exists
                    try:
                        if logger:
                            logger.info(f"Attempting to reset auto-increment sequence for table: {table_name}")
                        conn.execute(text(f"ALTER SEQUENCE {table_name}_id_seq RESTART WITH 1"))
                        if logger:
                            logger.info(f"Successfully reset auto-increment sequence for table '{table_name}'")
                    except Exception as seq_error:
                        if logger:
                            logger.debug(f"No sequence to reset for table '{table_name}' or sequence reset failed: {seq_error}")
                    
                    if logger:
                        logger.info(f"Table truncation completed successfully for: {table_name}")
                    return
                    
                except Exception as delete_error:
                    error_msg = f"DELETE operation also failed for table '{table_name}': {delete_error}"
                    if logger:
                        logger.error(error_msg)
                        logger.error(f"Traceback: {traceback.format_exc()}")
                    raise Exception(error_msg)
                    
    except Exception as e:
        error_msg = f"All truncation methods failed for table '{table_name}': {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)

def verify_table_exists(engine, table_name: str, logger=None) -> bool:
    """Verify if a table exists in the database."""
    try:
        if logger:
            logger.info(f"Verifying if table '{table_name}' exists")
        
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = :table_name
                )
            """), {"table_name": table_name})
            
            exists = result.fetchone()[0]
            if logger:
                logger.info(f"Table '{table_name}' exists: {exists}")
            return exists
            
    except Exception as e:
        error_msg = f"Error checking if table '{table_name}' exists: {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def verify_table_row_count(engine, table_name: str, logger=None) -> int:
    """Verify the current row count of a table."""
    try:
        if logger:
            logger.info(f"Verifying row count for table: {table_name}")
        
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.fetchone()[0]
            if logger:
                logger.info(f"Table '{table_name}' current row count: {count}")
            return count
            
    except Exception as e:
        error_msg = f"Error verifying row count for table '{table_name}': {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
        return -1

def check_table_locks(conn, table_name: str, logger=None) -> bool:
    """Check if table has any active locks that might prevent truncation."""
    try:
        if logger:
            logger.debug(f"Checking for active locks on table: {table_name}")
        
        # Query to check for locks on the specific table
        lock_query = text("""
            SELECT 
                pg_class.relname,
                pg_locks.locktype,
                pg_locks.mode,
                pg_locks.granted
            FROM pg_locks
            JOIN pg_class ON pg_locks.relation = pg_class.oid
            WHERE pg_class.relname = :table_name
        """)
        
        result = conn.execute(lock_query, {"table_name": table_name})
        locks = result.fetchall()
        
        if locks:
            if logger:
                logger.warning(f"Found {len(locks)} active locks on table '{table_name}'")
                for lock in locks:
                    logger.debug(f"Lock details: {lock}")
            return True
        else:
            if logger:
                logger.debug(f"No active locks found on table '{table_name}'")
            return False
            
    except Exception as e:
        if logger:
            logger.debug(f"Could not check locks for table '{table_name}': {str(e)}")
        return False

def close_engine(engine, logger=None):
    """Closes the SQLAlchemy engine connection pool."""
    try:
        if logger:
            logger.info("Closing SQLAlchemy engine connection pool")
        engine.dispose()
        if logger:
            logger.info("Successfully closed SQLAlchemy engine")
    except Exception as e:
        error_msg = f"Error closing SQLAlchemy engine: {str(e)}"
        if logger:
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")