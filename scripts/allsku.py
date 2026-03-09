import pandas as pd
import time
from sqlalchemy import text
import sys
import numpy as np
from datetime import timedelta, datetime
import logging
import os
import warnings
import traceback

# Add scripts folder to path
sys.path.append(r'C://Python_scripts')
warnings.filterwarnings("ignore")

try:
    from dagster import asset, AssetExecutionContext, MetadataValue
except ImportError:
    # Fallback for environments without Dagster
    def asset(*args, **kwargs):
        def decorator(func): return func
        return decorator(args[0]) if len(args) == 1 and callable(args[0]) else decorator
    class AssetExecutionContext: pass
    class MetadataValue: pass

from utils.log_file import LogManager
from utils.db_handler import *

# Set up logging
log_manager = LogManager("allsku")
logger = logging.getLogger(__name__)
logger.info("Starting allsku data processing script")

@asset(
    group_name="catalog",
    description="Extracts mv_allsku data from report service DB",
    compute_kind="python"
)
def allsku_extract(context: AssetExecutionContext) -> pd.DataFrame:
    """Fetch data from SQL database using the provided query file"""
    query_path = 'C://Python_scripts//query//allsku.sql'
    try:
        engine = create_sqlalchemy_engine_1(logger)
        df_sql = execute_sql_with_sqlalchemy(engine, query_path, logger)
        logger.info(f"Successfully retrieved {len(df_sql)} records from SQL database")
        
        # Add metadata to Dagster
        if hasattr(context, "add_output_metadata"):
            context.add_output_metadata({
                "rows_extracted": MetadataValue.int(len(df_sql)),
            })
            
        return df_sql
    except Exception as e:
        logger.error(f"Error fetching SQL data: {str(e)}")
        raise

@asset(
    group_name="catalog",
    description="Loads mv_allsku data into the local reporting DB",
    compute_kind="python"
)
def allsku_load(context: AssetExecutionContext, allsku_extract: pd.DataFrame) -> None:
    """Upload the processed dataframe to the target database"""
    df_sql = allsku_extract
    try:
        logger.info("Creating connection to target database")
        engine = create_sqlalchemy_engine_2(logger)
        
        # Verify table exists
        table_exists = verify_table_exists(engine, "allsku", logger)
        if not table_exists:
            # Note: order_master.py raises exception if it doesn't exist, 
            # but mv_allsku might be a new table or intended to be created.
            # However, following order_master logic:
            logger.warning("Target table 'allsku' does not exist. It will be created by to_sql.")
        
        # Verify initial table state
        logger.info("Checking initial table state")
        initial_count = verify_table_row_count(engine, "allsku", logger)
        logger.info(f"Initial row count: {initial_count}")
        
        # Truncate existing table using safe method
        if table_exists:
            logger.info("Starting table truncation")
            truncate_table_safe(engine, "allsku", logger)
            logger.info("Successfully completed table truncation")
            
            # Verify truncation was successful
            logger.info("Verifying truncation success")
            post_truncate_count = verify_table_row_count(engine, "allsku", logger)
            logger.info(f"Post-truncate row count: {post_truncate_count}")
            
            if post_truncate_count > 0:
                logger.warning(f"Table truncation may not have been complete. Rows remaining: {post_truncate_count}")
        else:
            post_truncate_count = 0

        # Upload data
        logger.info(f"Starting data upload - uploading {len(df_sql)} records")
        logger.info(f"DataFrame info - Shape: {df_sql.shape}, Columns: {list(df_sql.columns)}")
        
        # Check for any null values or data issues
        null_counts = df_sql.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(f"DataFrame contains null values: {null_counts[null_counts > 0].to_dict()}")
        
        # Perform the upload
        df_sql.to_sql(
            'allsku', 
            engine, 
            if_exists='replace', 
            index=False, 
            method='multi',
            chunksize=1000
        )
        logger.info(f"Successfully uploaded {len(df_sql)} records to database")
        
        # Verify final state
        logger.info("Verifying final upload state")
        final_count = verify_table_row_count(engine, "allsku", logger)
        logger.info(f"Final row count: {final_count}")
        
        # Summary
        logger.info(f"Data processing summary:")
        logger.info(f"  - Initial rows: {initial_count}")
        logger.info(f"  - Post-truncate rows: {post_truncate_count}")
        logger.info(f"  - Records uploaded: {len(df_sql)}")
        logger.info(f"  - Final rows: {final_count}")
        
        if final_count == len(df_sql):
            logger.info("Upload verification successful - row counts match")
        else:
            logger.warning(f"Upload verification warning - expected {len(df_sql)} rows, found {final_count}")
            
        # Add metadata to Dagster
        if hasattr(context, "add_output_metadata"):
            context.add_output_metadata({
                "initial_row_count": MetadataValue.int(initial_count),
                "rows_loaded": MetadataValue.int(len(df_sql)),
                "final_row_count": MetadataValue.int(final_count)
            })
        
    except Exception as e:
        error_msg = f"Error uploading to database: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)

if __name__ == '__main__':
    try:
        from dagster import build_asset_context
        
        logger.info("Starting standalone Dagster execution for allsku data processing")
        context = build_asset_context()
        
        df_sql = allsku_extract(context)
        
        if not df_sql.empty:
            logger.info("Starting data upload process")
            allsku_load(context, df_sql)
        else:
            logger.warning("No data retrieved from source, skipping load step.")
            
        logger.info("allsku data processing completed successfully")
        print("allsku Updated")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise