"""
3PL Script - Operations Data Processing
This script processes third-party logistics (3PL) data from multiple sources,
merges it with operational data, and uploads the results to a PostgreSQL database.
"""

import os
import pandas as pd
import psycopg2
import numpy as np
import logging
from datetime import datetime
import chardet
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings("ignore")
import sys
# Add utils folder to path
sys.path.append(r'C://Python_scripts')
from utils.log_file import LogManager

# Set up logging
log_manager = LogManager("3pl_data")
logger = logging.getLogger(__name__)
logger.info("Starting 3PL data processing script")

# Set your folder path
folder_path = r"C://Users//metabase//OneDrive - Shoplover Ltd//3pl_partner//"
logger.info("Using folder path: %s", folder_path)


# Function to get latest file matching a prefix
def get_latest_file(folder, prefix):
    files = [f for f in os.listdir(folder) if f.startswith(prefix)]
    if not files:
        return None
    full_paths = [os.path.join(folder, f) for f in files]
    latest_file = max(full_paths, key=os.path.getmtime)
    return latest_file

# Get latest files
logger.info("Searching for latest files")
deliveries_file = get_latest_file(folder_path, "deliveries_")
orders_file = get_latest_file(folder_path, "Orders")
issue_tracker_file = get_latest_file(folder_path, "[Cartup] Issue Tracker")

# Initialize DataFrames
df_pathao = None
df_carrybee = None
df_issue_tracker = None

# Load Deliveries File
if deliveries_file:
    logger.info("Loading deliveries file: %s", deliveries_file)
    df_pathao = pd.read_csv(deliveries_file)
    logger.info("Successfully loaded Pathao data with %d records", len(df_pathao))
    
    # Clean column names
    df_pathao.columns = (
        df_pathao.columns
        .str.replace(' ', '_')
        .str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
        .str.lower()
    )
else:
    logger.warning("No deliveries file found")

# Load Orders File
if orders_file:
    logger.info("Loading orders file: %s", orders_file)
    df_carrybee = pd.read_csv(orders_file)
    logger.info("Successfully loaded Carrybee data with %d records", len(df_carrybee))
else:
    logger.warning("No orders file found")

# Load Issue Tracker File
if issue_tracker_file:
    logger.info("Loading issue tracker file: %s", issue_tracker_file)

    try:
        # Handle based on file extension
        if issue_tracker_file.endswith('.csv'):
            with open(issue_tracker_file, 'rb') as f:
                result = chardet.detect(f.read(10000))
            encoding = result['encoding']
            logger.debug("Detected encoding: %s", encoding)
            df_issue_tracker = pd.read_csv(issue_tracker_file, encoding=encoding, header=1)

        elif issue_tracker_file.endswith(('.xlsx', '.xls')):
            df_issue_tracker = pd.read_excel(issue_tracker_file, sheet_name="Forward & Delivery Failed claim", header=1)
            
        else:
            logger.warning("Unsupported file format for issue tracker")
            df_issue_tracker = None

        if df_issue_tracker is not None:
            df_issue_tracker.columns = (
                df_issue_tracker.columns
                .str.replace(' ', '_')
                .str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
                .str.lower()
            )
            logger.info("Successfully loaded issue tracker data with %d records", len(df_issue_tracker))
    except Exception as e:
        logger.error("Error loading issue tracker file: %s", str(e))
        raise
else:
    logger.warning("No issue tracker file found")

if df_carrybee is not None and not df_carrybee.empty:
    df_carrybee['rank'] = (
        df_carrybee.groupby('merchant_order_id')['order_created_at']
        .rank(method='first', ascending=False)
        .astype(int)
    )

    df_carrybee = df_carrybee[df_carrybee['rank'] == 1]
    df_carrybee = df_carrybee.drop('rank', axis=1)
    logger.info("Carrybee DataFrame processed, duplicates removed")
else:
    logger.warning("Carrybee DataFrame is empty or not loaded")

if df_pathao is not None and not df_pathao.empty:
    df_pathao['rank'] = (
        df_pathao.groupby('merchant_order_id')['order_created_at']
        .rank(method='first', ascending=False)
        .astype(int)
    )

    df_pathao = df_pathao[df_pathao['rank'] == 1]
    df_pathao = df_pathao.drop('rank', axis=1)
    logger.info("Pathao DataFrame processed, duplicates removed")
else:
    logger.warning("Pathao DataFrame is empty or not loaded")

def clean_merchand_order_id(df, column='merchand_order_id'):
    if column in df.columns:
        df[column] = df[column] .astype(str).str.strip('"').str.strip("'")
        logger.info("Merchant order ID cleaned for column: %s", column)
    else:
        logger.warning("Column %s not found in DataFrame for cleaning", column)
    return df

def convert_timestamp_columns(df, columns=[]):
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info("Converted column to timestamp: %s", col)
        else:
            logger.warning("Column %s not found in DataFrame for timestamp conversion", col)
    return df

def convert_excel_date(val):
    """
    Converts Excel serial date number to datetime, leaves strings untouched.
    """
    if isinstance(val, str):
        return pd.to_datetime(val)  # Already a string, no need for special handling
    elif pd.isna(val):  # Handle NaN values
        return pd.NaT
    elif np.issubdtype(type(val), np.number):
        # Excel date starts from 1899-12-30 (due to 1900 leap year bug)
        return pd.to_datetime('1899-12-30') + pd.Timedelta(days=val)
    else:
        return pd.NaT  # Fallback for other types of values
    
# Cleaning & sorting dataframes 
logger.info("Cleaning and sorting dataframes")
df_carrybee = clean_merchand_order_id(df_carrybee, 'merchant_order_id')
df_carrybee = convert_timestamp_columns(df_carrybee, ['order_created_at','order_status_updated_at'])
df_carrybee['partner_name'] = 'carrybee'

df_pathao = clean_merchand_order_id(df_pathao, 'merchant_order_id')
df_pathao = convert_timestamp_columns(df_pathao, ['order_created_at','order_status_updated_at'])
df_pathao['partner_name'] = 'pathao'


# merge both dataframes into one dataframe 
logger.info("Merging Carrybee and Pathao dataframes")
# Get common columns
common_columns = df_carrybee.columns.intersection(df_carrybee.columns)
# Filter both dataframes to include only common columns
df_carrybee_common = df_carrybee[common_columns]
df_pathao_common = df_pathao[common_columns]
# Concatenate the two dataframes
merged_df = pd.concat([df_carrybee_common, df_pathao_common], ignore_index=True)
merged_df = merged_df.sort_values(by=['order_created_at'], ascending=[True])

merged_df['rank'] = (
    merged_df.groupby('merchant_order_id')['order_created_at']
    .rank(method='first', ascending=False)
    .astype(int)
)

merged_df = merged_df[merged_df['rank'] == 1]
merged_df = merged_df.drop('rank', axis=1)
merged_df = merged_df.rename(columns={
            "merchant_order_id": "package_code",
            "order_created_at": "booking_date"
        })
merged_df.to_csv(folder_path + 'ops_3pl_file.csv', index=False)
logger.info("Merged DataFrame saved to CSV: %sops_3pl_file.csv", folder_path)

# Step 1: Filter rows with duplicated merchant_order_id (keep all instances)
duplicates_df = merged_df[merged_df.duplicated(subset=['booking_date'], keep=False)]

# Step 2: Sort by merchant_order_id and order_created_at for readability
duplicates_sorted_df = duplicates_df.sort_values(by=['package_code', 'booking_date'])
logger.info("Identified %d duplicate rows based on booking_date", len(duplicates_df))

# --- Database configuration ---
logger.info("Connecting to database")
db_config = {
    "dbname": "report_service",
    "user": "anonna_sll_88437_cartup",
    "password": "#ehrnFhsb&52b%J2K#",
    "host": "172.31.77.37",
    "port": "5432"
}

# --- Connect to database ---
try:
    conn = psycopg2.connect(**db_config)
    logger.info("Connected to the database successfully")
except Exception as e:
    logger.error("Error connecting to the database: %s", str(e))
    exit()

logger.info("Loading SQL query from file")
with open('C://Python_scripts//query//all_3pl.sql', 'r') as file:
    sql_query = file.read()
    
logger.info("Executing SQL query to fetch operations data")
try:
    ops_data = pd.read_sql(sql_query, conn)
    logger.info("Successfully fetched %d records from database", len(ops_data))
except Exception as e:
    logger.error("Error executing SQL query: %s", str(e))
    raise

ops_data = ops_data.rename(
    columns={
        "PackageCode": "package_code",
        "OriginalPackageCode": "original_package_code",
        "OrderNo": "order_no",
        "OrderCode": "order_code",
        "OrderReturnCode": "order_return_code",
        "Quantity": "quantity",
        "TotalPrice": "total_price",
        "TotalPackageWeightInKg": "total_package_weight_in_kg",
        "SellerType": "seller_type",
        "SellerCode": "seller_code",
        "SellerName": "seller_name",
        "ShopName": "shop_name",
        "FulfillmentCode": "fulfillment_by",
        "OrderType": "order_type",
        "JourneyType": "journey_type",
        "PackageStatus": "package_status",
        "SourceHub": "source_hub",
        "DestinationHub": "destination_hub",
        "CurrentLocation": "current_location",
        "CreatedAt": "created_at",
        "UpdatedAt": "updated_at",
    }
)
ops_data = convert_timestamp_columns(ops_data, ["created_at", "updated_at"])
logger.info("Operational data loaded from database and columns renamed")

merged_with_ops = pd.merge(ops_data, merged_df[['package_code', 'partner_name', 'booking_date','order_status']],on='package_code',how='left')
# Calculate aging and process journey types
logger.info("Processing package aging and journey types")

# Ensure booking_date is datetime and handle missing values
merged_with_ops['booking_date'] = pd.to_datetime(merged_with_ops['booking_date'], errors='coerce')
now = pd.Timestamp.now()
time_diff = now - merged_with_ops['booking_date']
merged_with_ops['aging'] = (
    (time_diff.dt.total_seconds() / (3600 * 24)).round().fillna(0).astype(int)
)
logger.info("Aging calculation completed")

# Define the status lists
logger.info("Classifying journey types")
forward_statuses = [
    'Pickup Requested',
    'On the Way To Delivery Hub',
    'Assigned For Delivery',
    'Received at Last Mile HUB',
    'On the way to central warehouse',
    'On hold',
    'At central warehouse',
    'Paid Return',
    'Pickup Cancel',
    'Partial Delivery',
    'Damaged',
    'Lost'
]

reverse_statuses = [
    'Return Pending from HUB',
    'Return On the way to Central sorting',
    'Return At Sorting Hub',
    'Returning to Sorting HUB',
    'Return',
    'Return on the way to Merchant',
    'Return At Central Sorting',
    'Return Received by Sorting Hub',
    'Returned To Merchant',
    'Return at First Mile',
    'Returned to Inventory'
]

# Add the new column 'journey_type_3pl'
merged_with_ops['journey_type_3pl'] = np.where(
    merged_with_ops['order_status'].isin(forward_statuses),
    'forward_pending',
    np.where(
        merged_with_ops['order_status'].isin(reverse_statuses),
        'reverse_pending',
        'unknown'
    )
)

# Log journey type statistics
journey_types = merged_with_ops['journey_type_3pl'].value_counts()
logger.info("Journey type distribution:")
for journey_type, count in journey_types.items():
    logger.info("  %s: %d packages", journey_type, count)

# Create engine
engine = create_engine('postgresql://postgres:admin123@172.16.90.18:5432/postgres')

# Truncate the table using text()
logger.info("Truncating table pl_3pl_package_status in database")
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE pl_3pl_package_status"))
    conn.commit()  # Important to commit if autocommit is off
logger.info("Table truncated successfully")

# Upload DataFrame to Postgres
logger.info("Uploading merged data to pl_3pl_package_status table")
merged_with_ops.to_sql('pl_3pl_package_status', engine, if_exists='append', index=False)
logger.info("Successfully uploaded %d records to database", len(merged_with_ops))
logger.info("3PL data processing script completed successfully")
print("3pl Data Updated")