import pandas as pd
import numpy as np 
from datetime import datetime, date
import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
# Add utils folder to path
sys.path.append(r'C://Python_scripts')
import warnings
warnings.filterwarnings("ignore")
import logging
from utils.log_file import LogManager
from utils.db_handler import *


# Set up logging
log_manager = LogManager("mkt_com_dump")
logger = logging.getLogger(__name__)
logger.info("="*80)
logger.info("Starting Marketing Com Dump data processing script")
logger.info("="*80)


logger.info("Loading SQL query from file")
try:
    with open('C://Python_scripts//query//mkt_com_dump.sql', 'r', encoding='utf-8') as file:
        sql_query = file.read()
    logger.info(f"Successfully loaded SQL query from file (Query length: {len(sql_query)} characters)")
except Exception as e:
    logger.error(f"Error loading SQL query file: {str(e)}")
    raise


def get_sql_data(query_path):
    """Fetch data from SQL database using the category tree query"""
    try:
        logger.info(f"Creating SQLAlchemy engine for database connection")
        engine = create_sqlalchemy_engine_1()
        logger.info("Successfully created database engine")
        
        logger.info(f"Executing SQL query from: {query_path}")
        df_sql = execute_sql_with_sqlalchemy(engine, query_path)
        
        logger.info(f" Successfully retrieved {len(df_sql)} records from SQL database")
        logger.info(f"  - Columns: {len(df_sql.columns)}")
        logger.info(f"  - Data types: {dict(df_sql.dtypes)}")
        logger.info(f"  - Memory usage: {df_sql.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        return df_sql
    except Exception as e:
        logger.error(f"Error fetching SQL data: {str(e)}")
        raise


def authenticate_gaccount(service_account_file="C://Python_scripts//service_account.json"):
    """Authenticate with Google Sheets using service account credentials"""
    try:
        logger.info(f"Attempting to authenticate with Google Sheets")
        logger.info(f"  - Service account file: {service_account_file}")
        
        scope = ["https://spreadsheets.google.com/feeds", 
                 "https://www.googleapis.com/auth/drive"]
        logger.debug(f"Scopes: {scope}")
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            service_account_file, scope)
        logger.debug("Service account credentials loaded successfully")
        
        client = gspread.authorize(creds)
        logger.info(" Successfully authenticated with Google Sheets")
        
        return client
    except Exception as e:
        logger.error(f"Error authenticating with Google Sheets: {str(e)}")
        raise


# Authenticate once at module level
logger.info("-"*80)
logger.info("AUTHENTICATION PHASE")
logger.info("-"*80)
try:
    gspread_client = authenticate_gaccount()
    logger.info(" Module-level authentication successful")
except Exception as e:
    logger.error(f"Failed to authenticate at module level: {str(e)}")
    gspread_client = None


def upload_to_google_sheets(df, spreadsheet_id, sheet_name=None, sheet_index=0, client=None):
    """Upload DataFrame to Google Sheets"""
    try:
        logger.info("-"*80)
        logger.info("UPLOAD PHASE")
        logger.info("-"*80)
        
        # Validate client
        if client is None:
            client = gspread_client
        
        if client is None:
            raise ValueError("Google Sheets client not authenticated")
        
        logger.info(f"Using {'provided' if client is not None else 'module-level'} client")
        
        # Log data info before upload
        logger.info(f"Preparing data for upload")
        logger.info(f"  - Records to upload: {len(df)}")
        logger.info(f"  - Columns: {len(df.columns)}")
        logger.info(f"  - Column names: {list(df.columns)}")
        logger.info(f"  - Data size: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Open the spreadsheet and worksheet
        logger.info(f"Opening Google Sheets spreadsheet")
        logger.info(f"  - Spreadsheet ID: {spreadsheet_id}")
        
        spreadsheet = client.open_by_key(spreadsheet_id)
        logger.debug(f"Spreadsheet opened: {spreadsheet.title}")
        
        # Open worksheet by name or index
        if sheet_name:
            logger.info(f"  - Worksheet name: {sheet_name}")
            sheet = spreadsheet.worksheet(sheet_name)
            logger.debug(f"Worksheet selected by name: {sheet.title}")
        else:
            logger.info(f"  - Worksheet index: {sheet_index}")
            sheet = spreadsheet.get_worksheet(sheet_index)
            logger.debug(f"Worksheet selected by index: {sheet.title}")
        
        # Convert entire dataframe to strings for JSON serialization
        logger.info(f"Converting DataFrame to strings for JSON compatibility")

        
        df_clean = df.copy()
        
        # Convert all columns to string type
        for col in df_clean.columns:
            try:
                df_clean[col] = df_clean[col].astype(str)
            except Exception as e:
                logger.warning(f"Could not convert column '{col}': {str(e)}")
        
        # Fill NaN values
        df_clean = df_clean.fillna('')
        
        # Replace 'NaT' and 'None' strings with empty string
        df_clean = df_clean.replace({'NaT': '', 'None': ''})
        
        null_count = df.isnull().sum().sum()
        logger.info(f"  - Null values found: {null_count}")
        logger.info(f"  - All columns converted to string format")
        
        # Clear existing data and inject new data
        logger.info(f"Clearing existing sheet data")
        sheet.clear()
        logger.debug(f"Sheet cleared successfully")        
        logger.info(f"Uploading {len(df_clean)} records to Google Sheets")
        # Prepare data as list of lists
        # data_to_upload = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
        # sheet.update(data_to_upload)

        data_values = df_clean.astype(object).where(pd.notnull(df_clean), None).values.tolist()
        data_to_upload = [df_clean.columns.tolist()] + data_values
        sheet.update(data_to_upload, value_input_option="USER_ENTERED")
        
        logger.info(f" Successfully uploaded {len(df_clean)} records to Google Sheets")
        logger.info(f"  - Columns uploaded: {len(df_clean.columns)}")
        logger.info(f"  - Rows uploaded: {len(df_clean)}")
        logger.info(f"  - Total cells: {len(df_clean) * len(df_clean.columns)}")
        
        print(f" Data successfully uploaded! {len(df_clean)} rows")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading to Google Sheets: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise



if __name__ == '__main__':
    try:
        logger.info("="*80)
        logger.info("MAIN EXECUTION STARTED")
        logger.info("="*80)
        
        query_path = 'C://Python_scripts//query//mkt_com_dump.sql'
        spreadsheet_id = "1YYNZXrQiKfwgxzuDy1FfdeBGagxeB8djCYaaLHsk03A"
        sheet_name = "COM_Dump"  # Specify your sheet name here
        
        logger.info(f"Configuration:")
        logger.info(f"  - Query path: {query_path}")
        logger.info(f"  - Spreadsheet ID: {spreadsheet_id}")
        logger.info(f"  - Sheet name: {sheet_name}")
        
        # Get data from source database
        logger.info("-"*80)
        logger.info("EXTRACTION PHASE")
        logger.info("-"*80)
        logger.info("Starting data extraction process")
        df_sql = get_sql_data(query_path)
        
        # Upload to Google Sheets
        logger.info("")
        logger.info("Starting data upload to Google Sheets")
        upload_to_google_sheets(df_sql, spreadsheet_id, sheet_name=sheet_name)
        
        logger.info("="*80)
        logger.info(" Marketing Com Dump Updated Successfully")
        logger.info("="*80)
        print(" Marketing Com Dump Updated Successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        logger.error("="*80)
        raise