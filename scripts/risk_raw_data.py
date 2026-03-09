import pandas as pd
import numpy as np
import time
import sys
import traceback
from datetime import datetime
from sqlalchemy import inspect, text

# Add utils folder to path
sys.path.append(r'C://Python_scripts')
import warnings
warnings.filterwarnings("ignore")
import logging
from utils.log_file import LogManager
from utils.db_handler import *

# Set up logging
log_manager = LogManager("risk_raw_data")
logger = logging.getLogger(__name__)
logger.info("Starting Risk Raw Data processing script")

# =====================================================
# DATABASE CONNECTIONS
# =====================================================
logger.info("Initializing database connections")
SOURCE_ENGINE = create_sqlalchemy_engine_1()
TARGET_ENGINE = create_sqlalchemy_engine_2(logger)
logger.info("Database connections initialized successfully")


# =====================================================
# LOAD SQL QUERIES
# =====================================================
logger.info("Loading SQL queries from files")

# Note: Load SQL queries from external files for better organization
# For now, queries are defined inline. Consider moving to separate .sql files
# in the query/ folder for production use

sql_order_master = """ 
with order_sku_bridge AS (
    SELECT
        o."Id" AS "OrderId",
        o."OrderReferenceId" as "CustomerOrderCode",
		o."VoucherId",
		o."RequestedDeviceId",
		o."ActualShippingCost",
		o."TotalWeightInKg",
        o."FreeShippingId",
        oi."ShopSKU"
 
    FROM public."vw_Orders" o
    JOIN public."vw_OrderItems" oi
      ON o."Id" = oi."OrderId"
	  ),
product_stock AS (
    SELECT "ShopSKU", "CurrentStockQty"
    FROM public."vw_ProductVariant"
),
 logistics_base AS (
    SELECT
        "OrderId",
        "ShopSKU",
        "OrderDate",
        "CustomerOrderCode",
        "SellerOrderCode",
        "CustomerId",
        "CustomerName",
        "ShippingState",
        "ShippingCity",
        "ShippingZone",
        "SourceHubId",
        "DestinationHub",
		"DestinationHubId",
        "SellerId",
        "SellerCode",
        "ShopName",
        "Seller Type",
        "SourceHub",
		"WeightInKg",
        "LogisticStatus",
        "CustomerStatus",
        "SellerStatus",
        "ProductId",
        "VariantId",
        "ProductName",
        "VariantName",
        "CategoryId",
        "Quantity" as "OrderQuantity",
        "CancelReason",
        "LastUpdated",
        "PickUpRiderId",
        "DeliveryRiderId",
        "Delivery Rider Full Name",
        "Delivery Rider Phone",
        "Delivery Rider Status",
        "FailedDeliverReason",
        "CancelledDate",
        CAST("Delivered" AS DATE) AS "DeliveredDate"
    FROM forward_logistics
   -- WHERE "ProductName" NOT ILIKE '%Packaging Material%'
),

order_financials AS (
    SELECT
        "CustomerOrderCode",
        "ShopSKU",
        "TotalPrice",
        "ShippingCost",
        "VoucherDiscount",
        "SellerDiscount",
        "CartupDiscount",
        "TransactionAmount",
        "OrderValue",
        "PaymentMethod",
        "IsPaid",
        "DiscountAmount",
        "VoucherDescription"
    FROM public.mv_order
),
shipping_info AS (
    SELECT
        "OrderId",
        "Name"    AS "Receiver Name",
        "Address" AS "Receiver Address",
        "Phone"   AS "Receiver Phone"
    FROM public."vw_ShippingInfos"
)

SELECT
    l."OrderDate",
    l."OrderId",
    l."CustomerOrderCode",
    l."SellerOrderCode",
    l."CustomerId",
    l."CustomerName",
    l."ShippingState",
    l."ShippingCity",
    l."ShippingZone",
    l."SourceHubId",
    l."DestinationHub",
    l."SellerId",
    l."SellerCode",
    l."ShopName",
	l."WeightInKg",
	osb."FreeShippingId",
    l."Seller Type",
    l."SourceHub",
    l."LogisticStatus",
    l."CustomerStatus",
    l."SellerStatus",
    l."ProductId",
    l."VariantId",
    l."ShopSKU",
    l."ProductName",
    l."VariantName",
    l."CategoryId",
    k."CurrentStockQty",
    l."OrderQuantity",
    f."TotalPrice",
    f."ShippingCost",
    osb."VoucherId",
	osb."ActualShippingCost",
	osb."TotalWeightInKg",
    f."VoucherDiscount",
    f."SellerDiscount",
    f."CartupDiscount",
    f."TransactionAmount",
    f."OrderValue",
    f."PaymentMethod",
    f."IsPaid",
    f."DiscountAmount",
    f."VoucherDescription",
    l."CancelReason",
    l."LastUpdated",
    l."PickUpRiderId",
    l."DeliveryRiderId",
    l."Delivery Rider Full Name",
    l."Delivery Rider Phone",
    l."Delivery Rider Status",
    l."FailedDeliverReason",
    s."Receiver Name",
    s."Receiver Address",
    s."Receiver Phone",
    osb."RequestedDeviceId",
    l."CancelledDate",
    l."DeliveredDate"
FROM logistics_base l 
JOIN order_sku_bridge osb ON l."OrderId" = osb."OrderId" AND l."ShopSKU" = osb."ShopSKU"
LEFT JOIN order_financials f  ON f."CustomerOrderCode" = osb."CustomerOrderCode" AND f."ShopSKU" = osb."ShopSKU"
LEFT JOIN shipping_info s ON s."OrderId" = l."OrderId"
LEFT JOIN product_stock k  ON k."ShopSKU" = l."ShopSKU"
--where "SellerId" not in (1,3)

"""

sql_category_tree = """
with category_tree as (
WITH "L1" AS (
	    SELECT 
	        "Id",
	        "IndustryId",
	        "NameEn"
	    FROM public."vw_Category"
	    WHERE "ParentId" IS NULL
	),
	"L2" AS (
	    SELECT 
	        L1."NameEn" AS cat1,
	        COALESCE(L2."Id", L1."Id") AS "Id",
	        COALESCE(L2."IndustryId", L1."IndustryId") AS "IndustryId",
	        L2."NameEn" AS cat2
	    FROM public."vw_Category" L2
	    RIGHT JOIN "L1" L1 ON L1."Id" = L2."ParentId"
	),
	"L3" AS (
	    SELECT 
	        cat1,
	        cat2,
	        COALESCE(L3."Id", L2."Id") AS "Id",
	        COALESCE(L3."IndustryId", L2."IndustryId") AS "IndustryId",
	        L3."NameEn" AS cat3
	    FROM public."vw_Category" L3
	    RIGHT JOIN "L2" L2 ON L2."Id" = L3."ParentId"
	),
	"L4" AS (
	    SELECT 
	        cat1,
	        cat2,
	        cat3,
	        COALESCE(L4."Id", L3."Id") AS "Id",
	        COALESCE(L4."IndustryId", L3."IndustryId") AS "IndustryId",
	        L4."NameEn" AS cat4
	    FROM public."vw_Category" L4
	    RIGHT JOIN "L3" L3 ON L3."Id" = L4."ParentId"
	),
	"L5" AS (
	    SELECT 
	        cat1,
	        cat2,
	        cat3,
	        cat4,
	        COALESCE(L5."Id", L4."Id") AS "Id",
	        COALESCE(L5."IndustryId", L4."IndustryId") AS "IndustryId",
	        L5."NameEn" AS cat5
	    FROM public."vw_Category" L5
	    RIGHT JOIN "L4" L4 ON L4."Id" = L5."ParentId"
	),
	"L6" AS (
	    SELECT 
	        COALESCE(L6."IndustryId", L5."IndustryId") AS vertical,
	        cat1,
	        cat2,
	        cat3,
	        cat4,
	        cat5,
	        L6."NameEn" AS cat6,
	        COALESCE(L6."Id", L5."Id") AS "Id"
	    FROM public."vw_Category" L6
	    RIGHT JOIN "L5" L5 ON L5."Id" = L6."ParentId"
	)
	SELECT
	    v."Name" AS "Vertical",
	    cat1,
	    cat2,
	    cat3,
	    cat4,
	    cat5,
	    cat6,
	    l."Id" AS "CategoryId",
	    COALESCE(cat6, cat5, cat4, cat3, cat2, cat1) AS deepestcategory
	FROM "L6" l
	LEFT JOIN public."vw_Industries" v ON l."vertical" = v."Id"
	ORDER BY "Vertical"
	)
    select * from category_tree 

"""

sql_return_master = """
 with return_bridge AS (
    SELECT
        a."OrderReferenceId",
        a."Id" AS "ReturnId",
        b."VariantId"
    FROM public."vw_OrderReturns" a
    JOIN public."vw_OrderReturnItems" b
        ON a."Id" = b."OrderReturnId"
),

return_master AS (
	  
	SELECT * from(
	select
    cast(r."ReturnCreatedDate" as date) AS "ReturnCreatedDate",
	r."RTM Delivery Rider Id",
    CAST(c."CreatedAt" AS DATE) AS "ReturnPkgCreatedDate",
    r."CustomerOrderCode",
    r."ReturnReason",
    r."IsFailedDelivery",
    rb."VariantId",
    SUM("Quantity") AS "Quantity",
    r."IsQC",
	"RTM Delivery Rider Name",
	"RTM Delivery Rider Full Name",
	"RTC Delivery Rider Name",
	"RTC Delivery Rider Full Name",
	CASE WHEN r."IsFailedDelivery" = FALSE THEN ROW_NUMBER() OVER (
		    PARTITION BY r."CustomerOrderCode",rb."VariantId"
		    ORDER BY c."CreatedAt" DESC) ELSE 1 END
				 AS "Rank"

FROM return_bridge rb

JOIN public."vw_OrderReturns" a ON rb."ReturnId" = a."Id"
LEFT JOIN mv_return_logistics r ON rb."OrderReferenceId" = r."CustomerOrderCode" AND rb."VariantId" = r."VariantId"
LEFT JOIN public."vw_OrderReturnPackages" c ON a."Id" = c."OrderReturnId"
LEFT JOIN "vw_OrderReturnItems" b  ON rb."ReturnId" = b."OrderReturnId" AND rb."VariantId" = b."VariantId"
WHERE c."CreatedAt" IS NOT NULL
GROUP BY
    r."ReturnCreatedDate",
	r."RTM Delivery Rider Id",
    c."CreatedAt",
    r."CustomerOrderCode",
    r."ReturnReason",
    r."IsFailedDelivery",
    rb."VariantId",
    r."IsQC",
    "Return Confirm",
	"RTM Delivery Rider Name",
	"RTM Delivery Rider Full Name",
	"RTC Delivery Rider Name",
	"RTC Delivery Rider Full Name")t
	WHERE "Rank" = 1
	)

    select * from return_master
"""

sql_return_qty = """
with return_qty AS (
SELECT 	"CustomerOrderCode",
"VariantId",
SUM("ReturnQuantity") AS total_returns
from public.mv_return_logistics 
WHERE "IsFailedDelivery" IS FALSE
AND "IsQC" IS TRUE
GROUP BY "CustomerOrderCode",
	"VariantId")
select * from return_qty
"""

sql_fwd_pkg = """
with fwd_pkg AS (
SELECT
	"OrderId",
	"PackageCode",
    "SourceHubId",
	"DeliveryStatusId"
FROM "vw_OrderPackages"
WHERE "IsQC" IS FALSE
)
select * from fwd_pkg
"""

sql_free_shipping = """
with FreeShipping as(
select 
"Id" as "FreeShippinId",
"IsAdmin"
from public."vw_FreeShippingNews"
)
select * from FreeShipping
"""


# =====================================================
# HELPER FUNCTIONS
# =====================================================

def fetch_data_from_database(engine, sql_query, query_name, chunksize=10000):
    """Fetch data from SQL database and return as DataFrame"""
    try:
        logger.info(f"Fetching {query_name} from source database")
        
        # Try to read all at once first, if too large, use chunks
        df = pd.read_sql(text(sql_query), engine)
        logger.info(f"Successfully retrieved {len(df)} records for {query_name}")
        return df
    except Exception as e:
        logger.error(f"Error fetching {query_name}: {str(e)}")
        raise

def load_all_data():
    """Load all required data from source database with retries for the large order_master table"""
    max_retries = 3
    retry_delay = 10  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Starting data fetch from source database (Attempt {attempt}/{max_retries})")
            
            # Load order_master with chunking
            logger.info("Loading order_master data")
            order_chunks = []
            rows_loaded = 0
            for chunk in pd.read_sql(text(sql_order_master), SOURCE_ENGINE, chunksize=10000):
                order_chunks.append(chunk)
                rows_loaded += len(chunk)
            
            df_order_master = pd.concat(order_chunks, ignore_index=True)
            logger.info(f"order_master fully loaded ({rows_loaded} total rows)")
            
            # Load other datasets
            df_category_tree = fetch_data_from_database(SOURCE_ENGINE, sql_category_tree, "category_tree")
            df_return_master = fetch_data_from_database(SOURCE_ENGINE, sql_return_master, "return_master")
            df_return_qty = fetch_data_from_database(SOURCE_ENGINE, sql_return_qty, "return_qty")
            df_fwd_pkg = fetch_data_from_database(SOURCE_ENGINE, sql_fwd_pkg, "forward_package")
            df_free_shipping = fetch_data_from_database(SOURCE_ENGINE, sql_free_shipping, "free_shipping")
            
            logger.info("All data loaded successfully from source database")
            
            return df_order_master, df_category_tree, df_return_master, df_return_qty, df_fwd_pkg, df_free_shipping

        except Exception as e:
            logger.error(f"Error on attempt {attempt}: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Data loading failed.")
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise

def merge_datasets(df_order_master, df_category_tree, df_return_master, df_return_qty, df_fwd_pkg, df_free_shipping):
    """Merge all datasets into a single DataFrame"""
    try:
        logger.info("Starting dataset merges")
        
        final_df = df_order_master.merge(df_category_tree, how="left", on=["CategoryId"])
        logger.info("Merged category_tree")
        
        final_df = final_df.merge(
            df_free_shipping,
            how="left",
            left_on=["FreeShippingId"],
            right_on=["FreeShippinId"]
        )
        logger.info("Merged free_shipping")
        
        final_df = final_df.merge(
            df_return_master,
            how="left",
            left_on=["CustomerOrderCode", "VariantId"],
            right_on=["CustomerOrderCode", "VariantId"]
        )
        logger.info("Merged return_master")
        
        final_df = final_df.merge(
            df_fwd_pkg,
            how="left",
            on=["OrderId", "SourceHubId"]
        )
        logger.info("Merged forward_package")
        
        final_df = final_df.merge(
            df_return_qty,
            how="left",
            left_on=["CustomerOrderCode", "VariantId"],
            right_on=["CustomerOrderCode", "VariantId"]
        )
        logger.info("Merged return_qty")
        logger.info(f"Final merged dataset shape: {final_df.shape}")
        
        return final_df
    
    except Exception as e:
        logger.error(f"Error during merge operations: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def calculate_derived_columns(final_df):
    """Calculate derived columns for the dataset"""
    try:
        logger.info("Calculating derived columns")
        
        # ReturnQty
        final_df["ReturnQty"] = (
            final_df["total_returns"]
                .fillna(final_df["Quantity"])
                .clip(upper=final_df["Quantity"])
        )
        logger.info("Calculated ReturnQty")
        
        # EligibleForRefund
        final_df["EligibleForRefund"] = np.select(
            [
                (final_df["IsPaid"] == True)
                & final_df["CancelReason"].notna()
                & final_df["DeliveredDate"].isna()
                & final_df["IsFailedDelivery"].isna(),

                (final_df["IsFailedDelivery"] == True)
                & (~final_df["PaymentMethod"].isin(["COD"])),

                (final_df["IsFailedDelivery"] == False)
                & (final_df["IsQC"] == True),
            ],
            [
                final_df["CancelledDate"],
                final_df["ReturnCreatedDate"],
                final_df["ReturnPkgCreatedDate"],
            ],
            default=None
        )
        logger.info("Calculated EligibleForRefund")
        
        # Shipping calculations
        final_df["CustomerShipping"] = np.where(
            final_df["FreeShippingId"] == 0,
            (
                final_df["ActualShippingCost"]
                * final_df["WeightInKg"]
                * final_df["OrderQuantity"]
                / final_df["TotalWeightInKg"].replace(0, np.nan)
            ).round(2),
            0
        )

        final_df["CartupShipping"] = np.where(
            (final_df["FreeShippingId"].notna()) & (final_df["IsAdmin"] == True),
            (
                final_df["ActualShippingCost"]
                * final_df["WeightInKg"]
                * final_df["OrderQuantity"]
                / final_df["TotalWeightInKg"].replace(0, np.nan)
            ).round(2),
            0
        )

        final_df["SellerShipping"] = np.where(
            (final_df["FreeShippingId"].notna()) & (final_df["IsAdmin"] == False),
            (
                final_df["ActualShippingCost"]
                * final_df["WeightInKg"]
                * final_df["OrderQuantity"]
                / final_df["TotalWeightInKg"].replace(0, np.nan)
            ).round(2),
            0
        )
        
        logger.info("Calculated all shipping columns")
        
        return final_df
    
    except Exception as e:
        logger.error(f"Error calculating derived columns: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def add_null_columns(df):
    """Add required null columns to DataFrame"""
    try:
        logger.info("Adding null columns to dataset")
        
        new_columns = [
            "Month","Hour","OrderTimestamp","PackageCode","OrderReturnPackageCode",
            "IsCRMConfirm","IsSellerConfirm","ItemPrice","LogisticStatus","ReturnStatus",
            "PickUp Attempt Number","Delivery Attempt Number","VoucherCode","BinNumber",
            "CardLevel","VoucherName","BankShare","TransactionStatus","TrxID","CardNo",
            "FulfillmentBy","FailedDeliverReason","ReturnReasonDescription",
            "PickupCancelReason","ReturnPickUpCancelReason","CancelReasonDescription",
            "OrderReturnCode","OriginalPackageCode","ReturnConfirmDate",
            "ReturnUpdatedDate","Pending","Delivery Rider Name",
            "Return Delivery Rider Name"
        ]

        for col in new_columns:
            if col not in df.columns:
                df[col] = np.nan
        
        logger.info(f"Added {len(new_columns)} null columns")
        return new_columns
    
    except Exception as e:
        logger.error(f"Error adding null columns: {str(e)}")
        raise

def apply_filters_and_reorder(df, new_columns):
    """Apply final filters and reorder columns"""
    try:
        logger.info("Applying final filters")
        
        # Apply filters
        df = df[
            (~df["SellerId"].isin([1, 3])) &
            (
                df["deepestcategory"]
                    .str.lower()
                    .ne("packaging material")
                    .fillna(True)
            )
        ]
        
        logger.info(f"Rows after final filters: {len(df)}")
        
        logger.info("Reordering final columns")
        
        # Reorder columns
        df = df[
            [
                "OrderDate","CustomerOrderCode","SellerOrderCode","IsFailedDelivery","IsQC",
                "CustomerId","CustomerName","Receiver Name","Receiver Address",
                "Receiver Phone","RequestedDeviceId","ShippingState","ShippingCity",
                "ShippingZone","SourceHub","DestinationHub","SellerId","SellerCode",
                "ShopName","Seller Type","SellerStatus","CustomerStatus","ProductId",
                "VariantId","ShopSKU","ProductName","VariantName","CurrentStockQty",
                "Vertical","cat1","deepestcategory","OrderQuantity","TotalPrice",
                "ShippingCost","VoucherId","FreeShippingId",
                 "CustomerShipping","CartupShipping","SellerShipping",
                "VoucherDiscount","SellerDiscount","CartupDiscount",
                "OrderValue","PaymentMethod","DiscountAmount","VoucherDescription",
                "TransactionAmount","IsPaid","CancelReason","ReturnReason","LastUpdated",
                "ReturnQty","EligibleForRefund","PickUpRiderId","DeliveryRiderId",
                "Delivery Rider Full Name","Delivery Rider Phone","Delivery Rider Status",
                "RTM Delivery Rider Id","RTM Delivery Rider Name","RTM Delivery Rider Full Name",
                "RTC Delivery Rider Name","RTC Delivery Rider Full Name",
            ] + new_columns
        ]
        
        logger.info("Final columns reordered successfully")
        
        # Sort for deterministic load
        logger.info("Sorting dataset for deterministic load")
        df = df.sort_values(
            by=["OrderDate", "CustomerOrderCode"],
            kind="mergesort"
        ).reset_index(drop=True)
        
        return df
    
    except Exception as e:
        logger.error(f"Error applying filters and reordering: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def upload_with_progress(df, engine, table_name, schema, chunksize=10000):
    """Upload DataFrame to database with progress logging"""
    try:
        total = len(df)
        uploaded = 0
        
        logger.info(f"Starting upload of {total} rows to {schema}.{table_name}")

        for start in range(0, total, chunksize):
            chunk = df.iloc[start:start + chunksize]

            chunk.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                method=None
            )

            uploaded += len(chunk)
            # logger.info(f"Upload progress: {uploaded}/{total} rows")
        
        logger.info(f"Successfully uploaded {total} rows to database")
        
    except Exception as e:
        logger.error(f"Error uploading data to database: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def upload_to_database(final_df):
    """Upload processed dataset to target database"""
    try:
        logger.info("Starting upload to target database")
        
        # Check and drop existing table
        logger.info("Checking for existing target table")
        inspector = inspect(TARGET_ENGINE)

        if inspector.has_table("daily_order_report", schema="public"):
            logger.info("Existing table found, dropping it")
            with TARGET_ENGINE.begin() as conn:
                conn.execute(
                    text('DROP TABLE IF EXISTS public."daily_order_report"')
                )
            logger.info("Target table dropped successfully")

        # Recreate table from DataFrame schema
        logger.info("Recreating target table from DataFrame schema")
        final_df.head(0).to_sql(
            name="daily_order_report",
            con=TARGET_ENGINE,
            schema="public",
            if_exists="replace",
            index=False
        )
        logger.info("Target table structure created")

        # Load data
        upload_with_progress(
            final_df,
            TARGET_ENGINE,
            "daily_order_report",
            "public",
            chunksize=10000
        )
        
        logger.info("Upload to database completed successfully")
        logger.info(f"Total rows uploaded: {len(final_df)}")
        
    except Exception as e:
        error_msg = f"Error uploading to database: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(error_msg)


# =====================================================
# MAIN EXECUTION
# =====================================================
if __name__ == '__main__':
    start_time = datetime.now()
    try:
        logger.info("=" * 60)
        logger.info("Starting Risk Raw Data ETL Process")
        logger.info("=" * 60)
        
        # Load all required data
        logger.info("Step 1: Loading data from source database")
        df_order_master, df_category_tree, df_return_master, df_return_qty, df_fwd_pkg, df_free_shipping = load_all_data()
        
        # Merge datasets
        logger.info("Step 2: Merging datasets")
        final_df = merge_datasets(df_order_master, df_category_tree, df_return_master, df_return_qty, df_fwd_pkg, df_free_shipping)
        
        # Calculate derived columns
        logger.info("Step 3: Calculating derived columns")
        final_df = calculate_derived_columns(final_df)
        
        # Add null columns
        logger.info("Step 4: Adding null columns")
        new_columns = add_null_columns(final_df)
        
        # Apply filters and reorder
        logger.info("Step 5: Applying filters and reordering columns")
        final_df = apply_filters_and_reorder(final_df, new_columns)
        
        # Upload to database
        logger.info("Step 6: Uploading to target database")
        upload_to_database(final_df)
        
        # Log completion summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info("Risk Raw Data ETL Process Completed Successfully")
        logger.info(f"Total rows processed: {len(final_df)}")
        logger.info(f"Total execution time: {duration:.2f} seconds")
        logger.info("=" * 60)
        
        print("Risk Raw Data Updated")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
