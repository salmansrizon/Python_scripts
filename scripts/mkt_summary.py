import pandas as pd
import urllib.parse
import warnings
import sys
# Add utils folder to path
sys.path.append(r'C://Python_scripts')
warnings.filterwarnings("ignore")
import logging
from utils.log_file import LogManager
from utils.db_handler import *


# Set up logging
log_manager = LogManager("mkt_data_summary")
logger = logging.getLogger(__name__)
logger.info("Starting Marketing data processing script")


def get_google_sheets_data():
    """Fetch data from Google Sheets"""
    try:
        # Sheet info
        sheet_id = "1YYNZXrQiKfwgxzuDy1FfdeBGagxeB8djCYaaLHsk03A"
        sheet_name = "Overall (Adj)"

        # URL encode the sheet name
        encoded_sheet_name = urllib.parse.quote(sheet_name)

        # Build the URL
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"

        # Read into DataFrame
        df = pd.read_csv(url)
        logger.info("Successfully retrieved %s records from Google Sheets", len(df))
        return df
    except Exception as e:
        logger.error("Error fetching Google Sheets data: %s", str(e))
        raise


# Rename duplicate columns to avoid duplicacy
def rename_duplicate_columns(df):
    """Rename duplicate columns by appending a suffix"""
    cols = df.columns.tolist()
    col_counts = {}
    new_cols = []
    
    for col in cols:
        # Strip whitespace for duplicate detection
        col_stripped = col.strip()
        
        if col_stripped in col_counts:
            col_counts[col_stripped] += 1
            new_col = f"{col}_{col_counts[col_stripped]}"
        else:
            col_counts[col_stripped] = 0
            new_col = col
        
        new_cols.append(new_col)
    
    df.columns = new_cols
    logger.info("Renamed duplicate columns where necessary")
    return df




def process_data(df):
    """Process and transform the raw data"""
    try:
        logger.info("Starting data processing and transformation")

        # Convert all column names to lowercase and remove extra spaces
        df.columns = df.columns.str.lower().str.strip()
        df.columns = df.columns.str.replace(" ", "_")
        df.columns = df.columns.str.replace("/", "_")
        df.columns = df.columns.str.replace("&", "and")
        df.columns = df.columns.str.replace("(", "").str.replace(")", "")
        logger.debug(f"Transformed column names: {df.columns.tolist()}")
        
        # Remove duplicate unnamed columns
        df = df.drop(columns=[col for col in df.columns if col.startswith("unnamed")], errors="ignore")
        
        # Handle date column safely
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # Process currency columns - clean then convert to numeric
        currency_columns = [
            "spend_taka", "usd", "spend_acquistion_top_funnel",
            "retargeting_mid_and_bottom_funnel", "branding_awareness",
            "cpm___1,000", "cpc", "cpi_overall", "paid.1", "cp_dau","paid.2",
            "cp_session", "cpb_gross", "cpb_net", "cac", "cprb",
            "cpo_gross", "cpo_net", "revenue", "aov", "cpo_overall",
            "paid_2", "cp_fb_purchase", "cp_gg_purchase", "exchange_rate","revenue__1","aov__1"
        ]
        for col in currency_columns:
            if col in df.columns:
                try:
                    # Remove common currency symbols and commas, then convert
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace("$", "", regex=False)
                        .str.replace("৳", "", regex=False)
                        .str.replace(",", "", regex=False)
                        .str.strip()
                    )
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    logger.debug(f"Processed currency column '{col}': sample = {df[col].dropna().head(1).tolist()}")
                except Exception as e:
                    logger.warning(f"Error processing currency column '{col}': {str(e)}")

        # Process percentage columns - divide by 100 only if values are > 1
        # First, identify actual percentage columns dynamically
        percentage_indicators = ["ctr", "cr", "click__dau", "dau___a2c", "a2c___checkout", "checkout___purchase"]
        actual_percentage_cols = [col for col in df.columns if any(ind in col for ind in percentage_indicators)]
        
        # Also add explicit percentage columns
        percentage_columns = [
            "ctr", "cti", "cr", "cir", "new_buyer_","dau_mau", "click___dau","new_buyer_%",
            "organic_%", "paid_%", "google_%___paid", "facebook_%___paid",
            "crm_", "others_"
        ]
        
        # Combine and deduplicate
        all_percentage_cols = list(set(actual_percentage_cols + percentage_columns))
        
        for col in all_percentage_cols:
            if col in df.columns:
                try:
                    # Remove percentage sign and commas, then convert
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace("%", "", regex=False)
                        .str.replace(",", "", regex=False)
                        .str.strip()
                    )
                    numeric_col = pd.to_numeric(df[col], errors="coerce")
                    
                    # Only divide by 100 if max value > 1 (indicating it's in percentage form)
                    if numeric_col.max() > 1:
                        df[col] = numeric_col / 100
                    else:
                        df[col] = numeric_col
                except Exception as e:
                    logger.warning(f"Error processing percentage column '{col}': {str(e)}")

        # Get all existing columns - maintain original order (not sorted)
        existing_columns = [col for col in df.columns if not col.startswith("unnamed")]
        logger.info(f"Processing {len(existing_columns)} columns")
        df = df[existing_columns]

        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

        # Rename columns to standardized format (using lowercase transformed column names)
        df = df.rename(
            columns={
                "date": "date",
                "day": "day",
                "campaign": "campaign",
                "spend_taka": "spend_taka",
                "usd": "spend_in_usd",
                "spend_acquistion_top_funnel": "spend_acquisition_top_funnel",
                "retargeting_mid_and_bottom_funnel": "spend_retargeting_mid_and_bottom_funnel",
                "branding_awareness": "spend_branding_awareness",
                "traffic_data_impressions": "impressions",
                "cpm___1,000": "cpm",
                "clicks": "clicks",
                "cpc": "cpc",
                "ctr": "ctr",
                "app_installs_overall": "app_installs_overall",
                "organic": "app_installs_organic",
                "paid": "app_installs_paid",
                "cpi_overall": "cpi_overall",
                "paid.1": "cpi_paid",
                "cti": "cti",
                "dau_web+app": "dau_web_and_app",
                "rtn_dau": "returning_dau",
                "new_dau": "new_dau",
                "app": "app_dau",
                "app_organic": "app_dau_organic",
                "app_paid": "app_dau_paid",
                "unpaid": "unpaid_dau",
                "cp_dau": "cost_per_dau",
                "click___dau": "click_per_dau",
                "drop-off": "drop_off_rate",
                "mau": "mau",
                "dau_mau": "dau_per_mau",
                "sessions": "sessions",
                "cp_session": "cost_per_session",
                "add_to_cart": "add_to_cart",
                "dau___a2c": "dau_per_add_to_cart",
                "checkout": "checkout",
                "a2c___checkout": "add_to_cart_per_checkout",
                "ga4_report_purchase": "ga4_reported_purchase",
                "new_purchase": "ga4_reported_new_purchase",
                "rtn_purchase": "ga4_reported_returning_purchase",
                "1_day_rtn_purchase": "ga4_reported_1_day_returning_purchase",
                "2_-_7_days_rtn_purchase": "ga4_reported_2_to_7_days_returning_purchase",
                "8_-_30_days_rtn_purchase": "ga4_reported_8_to_30_days_returning_purchase",
                "31_-_90_days_purchase": "ga4_reported_31_to_90_days_purchase",
                "business_data_cms_gross_buyers": "cms_reported_gross_buyers",
                "net_buyers": "cms_reported_net_buyers",
                "cpb_gross": "cms_reported_cost_per_buyer_gross",
                "cpb_net": "cms_reported_cost_per_buyer_net",
                "cr": "cms_reported_conversion_rate",
                "new_buyers": "cms_reported_new_buyers",
                "new_buyer_%": "cms_reported_new_buyer_percentage",
                "gross_orders": "cms_reported_gross_orders",
                "net_orders": "cms_reported_net_orders",
                "cpo_gross": "cms_reported_cost_per_order_gross",
                "cpo_net": "cms_reported_cost_per_order_net",
                "order___buyer": "cms_reported_order_per_buyer",
                "revenue": "cms_reported_gross_revenue",
                "aov": "cms_reported_average_order_value",
                "cir": "cms_reported_customer_investment_return",
                "adjust_data_purchase_orders_gross_orders": "adjust_gross_orders",
                "canceled_orders": "adjust_canceled_orders",
                "organic_%": "adjust_organic_order_percentage",
                "paid_%": "adjust_paid_order_percentage",
                "google_%___paid": "adjust_google_paid_order_percentage",
                "facebook_%___paid": "adjust_facebook_paid_order_percentage",
                "crm_%": "adjust_crm_order_percentage",
                "others_%": "adjust_others_order_percentage",
                "cpo_overall": "adjust_cpo_overall",
                "Paid.2": "adjust_paid_cpo",
                "cp_fb_purchase": "adjust_cp_fb_purchase_cpo",
                "cp_gg_purchase": "adjust_cp_gg_purchase_cpo",
                "checkout___purchase": "adjust_checkout_per_purchase",
                "revenue__1": "adjust_revenue",
                "aov__1": "adjust_aov",
                "roas": "adjust_roas",
            }
        )

        logger.info("Successfully processed %s records with %s columns", len(df), len(df.columns))
        logger.info("Final columns: %s", df.columns.tolist())
        return df

    except Exception as e:
        logger.error("Error processing data: %s", str(e))
        import traceback
        traceback.print_exc()
        raise

def upload_to_database(df_sql):
    """Upload the processed dataframe to the target database"""
    try:
        logger.info("Creating connection to target database")
        engine = create_sqlalchemy_engine_2(logger)
        
        # Verify table exists
        table_exists = verify_table_exists(engine, "daily_mkt_data_summary", logger)
        if not table_exists:
            logger.error("Target table 'daily_mkt_data_summary' does not exist")
            raise Exception("Target table 'daily_mkt_data_summary' does not exist")
        
        # Verify initial table state
        logger.info("Checking initial table state")
        initial_count = verify_table_row_count(engine, "daily_mkt_data_summary", logger)
        logger.info(f"Initial row count: {initial_count}")
        
        # Truncate existing table using safe method
        logger.info("Starting table truncation")
        truncate_table_safe(engine, "daily_mkt_data_summary", logger)
        logger.info("Successfully completed table truncation")
        
        # Verify truncation was successful
        logger.info("Verifying truncation success")
        post_truncate_count = verify_table_row_count(engine, "daily_mkt_data_summary", logger)
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
            'daily_mkt_data_summary', 
            engine, 
            if_exists='replace', 
            index=False, 
            method='multi',
            chunksize=1000
        )
        logger.info(f"Successfully uploaded {len(df_sql)} records to database")
        
        # Verify final state
        logger.info("Verifying final upload state")
        final_count = verify_table_row_count(engine, "daily_mkt_data_summary", logger)
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

if __name__ == "__main__":
    try:
        # Get data from Google Sheets
        logger.info("Starting data extraction process")
        df_raw = get_google_sheets_data()


        # Apply the renaming to df_raw
        logger.info("Renaming duplicate columns in raw data")
        df_raw = rename_duplicate_columns(df_raw)

        # Process the raw data
        logger.info("Starting data transformation process")
        df_processed = process_data(df_raw)

        # Upload to target database
        logger.info("Starting data upload process")
        upload_to_database(df_processed)

        logger.info("Marketing data processing completed successfully")

    except Exception as e:
        logger.exception("Error in main execution")
        raise
