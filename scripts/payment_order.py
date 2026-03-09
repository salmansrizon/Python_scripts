import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
# Add utils folder to path
sys.path.append(r"C://Python_scripts")
import traceback
import warnings
warnings.filterwarnings("ignore")
import logging
from utils.db_handler import *
from utils.log_file import LogManager

# Set up logging
log_manager = LogManager("Overall_Package_Journey")
logger = logging.getLogger(__name__)
logger.info("Starting Overall Package journey processing script")


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


def file_merging(df_sql):
    """Merge SQL data with CSV file data containing 3PL partner information"""
    try:
        # Define the directory
        folder_path = r"C://Users//metabase//OneDrive - Shoplover Ltd//3pl_partner//"
        logger.info("Searching for 3PL files in: %s", folder_path)

        # Find all CSV files in the folder
        csv_files = [
            f
            for f in os.listdir(folder_path)
            if f.endswith(".csv") and "ops_3pl_file" in f
        ]

        if not csv_files:
            logger.warning(
                "No file named 'ops_3pl_file.csv' found in '%s'", folder_path
            )
            return df_sql  # Return original data if no CSV file found
        else:
            # Use the first matching file
            csv_file = os.path.join(folder_path, csv_files[0])
            logger.info("Reading data from: %s", csv_file)

            try:
                # Read CSV with only required columns
                df_excel = pd.read_csv(
                    csv_file, usecols=["package_code", "partner_name", "booking_date"]
                )
                logger.info(
                    "Successfully loaded CSV data with %d records", len(df_excel)
                )

                # Convert all column names to snake_case
                df_excel.columns = (
                    df_excel.columns.str.replace(" ", "_")
                    .str.replace("/", "_")
                    .str.lower()
                )

                logger.debug("First 2 rows of loaded data:\n%s", df_excel.head(2))

            except Exception as e:
                logger.error("Error reading CSV file: %s", str(e))
                raise

        # Merge with Excel data on PackageCode
        logger.info("Merging SQL and CSV data")
        df_final = pd.merge(
            df_sql,
            df_excel[["package_code", "partner_name", "booking_date"]],
            on="package_code",
            how="left",
        )
        logger.info("Merged dataset contains %d records", len(df_final))

        return df_final

    except Exception as e:
        logger.error(f"Error in file merging: {str(e)}")
        raise


def processing_data(df_final):
    """Process the merged dataframe by adding cartup/3PL classification, weight categories,
    datetime conversions, and latest status determination"""
    try:
        logger.info("Starting data processing")

        # Adding Cartup/3PL column in the dataframe
        # ------------------------------------------------------
        # Define the condition in 3pl Hubs
        condition = df_final["destination_hub"].isin(
            ["3PL", "Patenga Hub", "Agrabad Hub", "Probartak", "Carry Bee"]
        )

        #  Add cartup_3pl column using the condition into the dataframe
        df_final["cartup_3pl"] = "cartup"  # Default value
        df_final.loc[condition, "cartup_3pl"] = "3pl"
        logger.info("Added cartup_3pl classification")

        # adding Bulkey column in the dataframe
        # ------------------------------------------------------
        # Convert 'TotalPackageWeightInKg' to float in case it's read as string
        df_final["total_package_weight_in_kg"] = df_final[
            "total_package_weight_in_kg"
        ].astype(float)

        # Apply the formula
        df_final["weight_category"] = pd.cut(
            df_final["total_package_weight_in_kg"],
            bins=[-float("inf"), 1, 4, 10, float("inf")],
            labels=["non bulky", "semi bulky", "bulky", "super bulky"],
        )
        logger.info("Added weight category classification")
        # List of datetime columns (in snake_case)
        datetime_cols = [
            "created_at",
            "updated_at",
            "in_pick_up",
            "first_pickup_attempt",
            "second_pickup_attempt",
            "third_pickup_attempt",
            "on_the_way_to_seller",
            "rts",
            "pickup_successful",
            "waiting_for_drop_off",
            "dropped_off",
            "pending_at_fm",
            "fm_mu_packed",
            "in_transit_to_sort",
            "pending_at_sort",
            "sort_mu_packed",
            "in_transit_to_lm",
            "pending_at_lm",
            "assigned_delivery_man",
            "received_by_delivery_man",
            "on_the_way_to_delivery",
            "first_delivery_attempt",
            "second_delivery_attempt",
            "third_delivery_attempt",
            "delivered",
            "canceled",
            "delivery_failed",
            "lm_mu_packed",
            "in_transit_to_fmr",
            "pending_at_fmr",
            "fmr_mu_pack_created",
            "in_transit_to_fm",
            "delivery_man_assign",
            "in_seller_delivery",
            "first_delivery_attempt_return_package",
            "second_delivery_attempt_return_package",
            "third_delivery_attempt_return_package",
            "returned_to_seller",
            "scrapped",
            "cr_pickup_successful",
            "in_transit_to_fmqcc",
        ]

        # Loop through each datetime column
        for col in datetime_cols:
            if col in df_final.columns:
                # Save original blank positions
                original_blanks = df_final[col].isna() | (df_final[col] == "")

                # Convert to datetime, coerce errors to NaT
                df_final[col] = pd.to_datetime(df_final[col], errors="coerce")

                # Replace NaT (invalid/coerced dates) back to blank string ''
                df_final.loc[original_blanks, col] = ""
        logger.info("Processed datetime columns")

        # find out the each package latest status
        # ---------------------------------------------
        # List of relevant timestamp columns (from start to end of the CRM process)
        status_hierarchy = [
            
            "in_pick_up",
            "first_pickup_attempt",
            "second_pickup_attempt",
            "third_pickup_attempt",
            "on_the_way_to_seller",
            "rts",
            "pickup_successful",
            "waiting_for_drop_off",
            "dropped_off",
            "pending_at_fm",
            "fm_mu_packed",
            "in_transit_to_sort",
            "pending_at_sort",
            "sort_mu_packed",
            "in_transit_to_lm",
            "pending_at_lm",
            "assigned_delivery_man",
            "received_by_delivery_man",
            "on_the_way_to_delivery",
            "first_delivery_attempt",
            "second_delivery_attempt",
            "third_delivery_attempt",
            "delivered",
            "delivery_failed",
            "lm_mu_packed",
            "in_transit_to_fmr",
            "pending_at_fmr",
            "fmr_mu_pack_created",
            "in_transit_to_fm",
            "delivery_man_assign",
            "in_seller_delivery",
            "first_delivery_attempt_return_package",
            "second_delivery_attempt_return_package",
            "third_delivery_attempt_return_package",
            "returned_to_seller",
            "scrapped",
            "cr_pickup_successful",
            "in_transit_to_fmqcc",
        ]

        # Create a dictionary to map status to priority level
        hierarchy_priority = {
            status: idx for idx, status in enumerate(status_hierarchy)
        }

        def get_latest_status(row):
            # Filter non-null timestamps
            valid_timestamps = {
                status: pd.to_datetime(row[status], errors="coerce")
                for status in status_hierarchy
                if pd.notna(row[status])
            }
            if not valid_timestamps:
                return None
            # Get the maximum timestamp
            max_timestamp = max(valid_timestamps.values())
            # Filter statuses with max timestamp
            candidates = [
                status for status, ts in valid_timestamps.items() if ts == max_timestamp
            ]
            # Return the status with the highest priority (latest in hierarchy)
            return max(candidates, key=lambda x: hierarchy_priority[x])

        # Add latest_status column
        df_final["latest_status"] = df_final.apply(get_latest_status, axis=1)
        logger.info("Added latest status determination")

        logger.info(
            f"Data processing completed successfully. Final dataframe shape: {df_final.shape}"
        )
        return df_final

    except Exception as e:
        logger.error(f"Error in data processing: {str(e)}")
        raise


def upload_to_database(df_sql):
    """Upload the processed dataframe to the target database"""
    try:
        logger.info("Creating connection to target database")
        engine = create_sqlalchemy_engine_2(logger)

        # Verify table exists
        table_exists = verify_table_exists(engine, "details_package_journey", logger)
        if not table_exists:
            logger.error("Target table 'details_package_journey' does not exist")
            raise Exception("Target table 'details_package_journey' does not exist")

        # Verify initial table state
        logger.info("Checking initial table state")
        initial_count = verify_table_row_count(
            engine, "details_package_journey", logger
        )
        logger.info(f"Initial row count: {initial_count}")

        # Truncate existing table using safe method
        logger.info("Starting table truncation")
        truncate_table_safe(engine, "details_package_journey", logger)
        logger.info("Successfully completed table truncation")

        # Verify truncation was successful
        logger.info("Verifying truncation success")
        post_truncate_count = verify_table_row_count(
            engine, "details_package_journey", logger
        )
        logger.info(f"Post-truncate row count: {post_truncate_count}")

        if post_truncate_count > 0:
            logger.warning(
                f"Table truncation may not have been complete. Rows remaining: {post_truncate_count}"
            )

        # Upload data
        logger.info(f"Starting data upload - uploading {len(df_sql)} records")
        logger.info(
            f"DataFrame info - Shape: {df_sql.shape}, Columns: {list(df_sql.columns)}"
        )

        # Check for any null values or data issues
        null_counts = df_sql.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(
                f"DataFrame contains null values: {null_counts[null_counts > 0].to_dict()}"
            )

        # Perform the upload
        df_sql.to_sql(
            "details_package_journey",
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
        logger.info(f"Successfully uploaded {len(df_sql)} records to database")

        # Verify final state
        logger.info("Verifying final upload state")
        final_count = verify_table_row_count(engine, "details_package_journey", logger)
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
            logger.warning(
                f"Upload verification warning - expected {len(df_sql)} rows, found {final_count}"
            )

    except Exception as e:
        error_msg = f"Error uploading to database: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)


if __name__ == "__main__":
    try:
        query_path = "C://Python_scripts//query//overall_pkg_journey.sql"
        # Get data from source database
        logger.info("Starting data extraction process")
        df_sql = get_sql_data(query_path)
        logger.info(f"Data extraction completed with {len(df_sql)} records")
        # Merge with 3PL partner data
        df_sql = file_merging(df_sql)
        logger.info(
            f"Data after merging with 3PL partner data contains {len(df_sql)} records"
        )
        # Process the data
        df_sql = processing_data(df_sql)
        logger.info(
            f"Data processing completed with final shape: {df_sql.shape}, columns: {list(df_sql.columns)}"
        )

        # Upload to target database
        logger.info("Starting data upload process")
        upload_to_database(df_sql)

        logger.info("Details Package Journey data processing completed successfully")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
print("Details Package Journey Updated")
