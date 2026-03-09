import pandas as pd
import numpy as np 
from datetime import datetime
import sys
# Add utils folder to path
sys.path.append(r'C://Python_scripts')
import warnings
warnings.filterwarnings("ignore")
import logging
from utils.log_file import LogManager
from utils.db_handler import *


# Set up logging
log_manager = LogManager("customer_risk_data")
logger = logging.getLogger(__name__)
logger.info("Starting Customer Risk data processing script")


logger.info("Loading SQL query from file")
with open('C://Python_scripts//query//customer_risk_data.sql', 'r', encoding='utf-8') as file:
    sql_query = file.read()


# Fetche data from Technonext Database usine category tree query
def get_sql_data(query_path):
    """Fetch data from SQL database using the category tree query"""
    try:
        engine = create_sqlalchemy_engine_1()
        df_sql = execute_sql_with_sqlalchemy(engine, query_path)
        logger.info(f"Successfully retrieved {len(df_sql)} records from SQL database")
        return df_sql
    except Exception as e:
        logger.error(f"Error fetching SQL data: {str(e)}")
        raise

def upload_to_database(df_sql):
    """Upload the processed dataframe to the target database"""
    try:
        logger.info("Creating connection to target database")
        engine = create_sqlalchemy_engine_2(logger)
        
        # Verify table exists
        table_exists = verify_table_exists(engine, "customer_risk_data", logger)
        if not table_exists:
            logger.error("Target table 'customer_risk_data' does not exist")
            raise Exception("Target table 'customer_risk_data' does not exist")
        
        # Verify initial table state
        logger.info("Checking initial table state")
        initial_count = verify_table_row_count(engine, "customer_risk_data", logger)
        logger.info(f"Initial row count: {initial_count}")
        
        # Truncate existing table using safe method
        logger.info("Starting table truncation")
        truncate_table_safe(engine, "customer_risk_data", logger)
        logger.info("Successfully completed table truncation")
        
        # Verify truncation was successful
        logger.info("Verifying truncation success")
        post_truncate_count = verify_table_row_count(engine, "customer_risk_data", logger)
        logger.info(f"Post-truncate row count: {post_truncate_count}")
        
        if post_truncate_count > 0:
            logger.warning(f"Table truncation may not have been complete. Rows remaining: {post_truncate_count}")
        
        # Upload data
        logger.info(f"Starting data upload - uploading {len(df_sql)} records")
        logger.info(f"DataFrame info - Shape: {df_sql.shape}, Columns: {list(df_sql.columns)}")
        
        # Check for any null values or data issues
        null_counts = df_sql.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(f"DataFrame contains null values: {null_counts[null_counts > 0].to_dict()}")
        
        # Perform the upload
        df_sql.to_sql(
            'customer_risk_data', 
            engine, 
            if_exists='replace', 
            index=False, 
            method='multi',
            chunksize=1000
        )
        logger.info(f"Successfully uploaded {len(df_sql)} records to database")
        
        # Verify final state
        logger.info("Verifying final upload state")
        final_count = verify_table_row_count(engine, "customer_risk_data", logger)
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
        
    except Exception as e:
        error_msg = f"Error uploading to database: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)

if __name__ == '__main__':
    try:
        query_path = 'C://Python_scripts//query//customer_risk_data.sql'
        # Get data from source database
        logger.info("Starting data extraction process")
        df_sql = get_sql_data(query_path)
        
        # Upload to target database
        logger.info("Starting data upload process")
        upload_to_database(df_sql)
        
        logger.info("Customer Risk data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
print("Customer Risk Updated")

