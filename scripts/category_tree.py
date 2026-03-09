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
log_manager = LogManager("category_tree")
logger = logging.getLogger(__name__)
logger.info("Starting Category tree data processing script")


logger.info("Loading SQL query from file")
with open('C://Python_scripts//query//category_tree.sql', 'r', encoding='utf-8') as file:
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
        engine = create_sqlalchemy_engine_2()
        
        # Truncate existing table
        with engine.connect() as conn:
            truncate_table(conn, "category_tree")
            logger.info("Successfully truncated target table")
        
        # Upload data
        df_sql.to_sql('category_tree', engine, if_exists='append', index=False)
        logger.info(f"Successfully uploaded {len(df_sql)} records to database")
        
    except Exception as e:
        logger.error(f"Error uploading to database: {str(e)}")
        raise

if __name__ == '__main__':
    try:
        query_path = 'C://Python_scripts//query//category_tree.sql'
        # Get data from source database
        logger.info("Starting data extraction process")
        df_sql = get_sql_data(query_path)
        
        # Upload to target database
        logger.info("Starting data upload process")
        upload_to_database(df_sql)
        
        logger.info("Category tree data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
print("category Tree Updated")

