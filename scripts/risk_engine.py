# %% [markdown]
# **Loading Libraries**

# %%
import os
import pandas as pd
import numpy as np
from datetime import datetime
from openpyxl import Workbook
import urllib.parse
import re
import networkx as nx
from collections import defaultdict
from sqlalchemy import create_engine, text
import sys
sys.path.append(r'C://Python_scripts')
from utils.log_file import LogManager

# Initialize logging
log_manager = LogManager()
logger = __import__('logging').getLogger(__name__)
logger.info("="*80)
logger.info("STARTING CUSTOMER SEGMENTATION SCRIPT")
logger.info("="*80)

# %% [markdown]
# **Read Data**

# %%
# Database configuration
db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin123",
    "host": "172.16.90.18",
    "port": "5432"
}
 
user = urllib.parse.quote_plus(db_config["user"])
password = urllib.parse.quote_plus(db_config["password"])
 
connection_url = f"postgresql+psycopg2://{user}:{password}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
 
try:
    engine = create_engine(connection_url)
    logger.info(f"Database connection successful to {db_config['host']}:{db_config['port']}")
except Exception as e:
    logger.error(f"Failed to connect to database: {str(e)}")
    raise

query = """
SELECT *
FROM public.daily_order_report;
"""

try:
    df = pd.read_sql(query, engine)
    logger.info(f"Query executed successfully. Loaded {len(df)} records with {len(df.columns)} columns")
except Exception as e:
    logger.error(f"Failed to execute query: {str(e)}")
    raise


# %% [markdown]
# **Filters and Cleaning**

# %%
logger.info("Starting data cleaning and filtering process")

#Drop Orders with Null Payment Method
initial_count = len(df)
df.dropna(subset=["PaymentMethod"], inplace=True)
logger.info(f"Dropped records with null PaymentMethod: {initial_count - len(df)} records removed, {len(df)} remaining")

before_packaging = len(df)
df = df[~df["deepestcategory"]

        .astype(str)

        .str.contains(r"\bpackaging materials?\b", case=False, na=False)]
logger.info(f"Filtered packaging materials: {before_packaging - len(df)} records removed, {len(df)} remaining")

# Remove Cartup Hub rows
before_cartup = len(df)
df = df[df["deepestcategory"].astype(str).str.strip().str.lower() != "cartup hub"]
logger.info(f"Filtered Cartup Hub: {before_cartup - len(df)} records removed, {len(df)} remaining")

before_seller = len(df)
df = df[df['SellerId'] != 1]
logger.info(f"Filtered SellerId == 1: {before_seller - len(df)} records removed, {len(df)} remaining")
cols = [
    "Receiver Address",
    "ShopName",
    "CustomerName",
    "Receiver Name",
    "ProductName"
]

# Count how many columns contain the word "test" per row
test_count = (
    df[cols]
    .apply(lambda col: col.astype(str).str.contains(r"\btest\b", case=False, na=False))
    .sum(axis=1)
)
 
# Keep rows where "test" appears in 0 or 1 column only
before_test = len(df)
df = df[test_count <= 1]
logger.info(f"Filtered test keyword appearances: {before_test - len(df)} records removed, {len(df)} remaining")
 
 
#-------------------Fix DataTypes----------------------------
def convert_if_numeric(col):
    # Work only on object (text) columns
    if col.dtype == "object":
        # Drop NaNs and check if all remaining values are digits
        non_null = col.dropna().astype(str)
 
        if non_null.str.fullmatch(r"\d+").all():
            # Convert to nullable integer
            return pd.to_numeric(col, errors="coerce").astype("Int64")
 
    return col
 
 
df = df.apply(convert_if_numeric)
 
# Convert IsFailedDelivery and IsQC to boolean
logger.info("Converting data types: IsFailedDelivery and IsQC to boolean")
df["IsFailedDelivery"] = df["IsFailedDelivery"].astype(str).str.lower().map({"true": True, "false": False})
df["IsQC"] = df["IsQC"].astype(str).str.lower().map({"true": True, "false": False})
logger.debug(f"Data type conversion completed")
 
#Fix Receiver Phone
logger.info("Cleaning Receiver Phone field")
def fix_phone(val):
    if not isinstance(val, str):
        val = str(val)
   
    val = val.strip()
   
    # Keep slicing off the first character until it starts with '1'
    while len(val) > 0 and not val.startswith('1'):
        val = val[1:]
    return val
 
df["Receiver Phone"] = df["Receiver Phone"].apply(fix_phone)
logger.info("Receiver Phone field cleaned")
 
# =============================
# Filter valid orders
# =============================
before_status_filter = len(df)
df_valid = df.loc[
    ~df['CustomerStatus'].str.lower().isin(
        ['canceled', 'cancelled', 'delivery failed']
    )
].copy()
logger.info(f"Filtered invalid customer status: {before_status_filter - len(df_valid)} records removed, {len(df_valid)} valid orders remaining")

# %% [markdown]
# **Risk Thresholds**

# %%
AccountThreshold=5
CancelReturnThreshold=0.6
InvalidReturnPercent=0.4
GISthreshold=5           # for Cancel and Return
DeepCatThreshold=15

# %% [markdown]
# **Bring VoucherCode based on VoucherID**

# %%
# ==========================
# Remove existing VoucherCode if present
# ==========================
df_valid = df_valid.copy()
df_valid.drop(columns=['VoucherCode'], errors='ignore', inplace=True)

# ==========================
# Load voucher sheets (include all relevant columns)
# ==========================
logger.info("Loading voucher data from Google Sheets")
BASE_URL = "https://docs.google.com/spreadsheets/d/1w1B2PavaRA2L2-plQ-nJJ8oK_1ffJw7zyGm4fiy7eWg/export?format=csv&gid="

try:
    # Load all columns to ensure Voucher Per User is included if needed
    v1 = pd.read_csv(BASE_URL + "0", header=1)
    v2 = pd.read_csv(BASE_URL + "1548310196", header=1)
    logger.info(f"Voucher sheets loaded successfully. v1: {len(v1)} rows, v2: {len(v2)} rows")
except Exception as e:
    logger.error(f"Failed to load voucher sheets: {str(e)}")
    raise

voucher_lookup = pd.concat([v1, v2], ignore_index=True)

# ==========================
# Normalize IDs (CRITICAL)
# ==========================
def normalize_id(s):
    return (
        s.astype("string")
         .str.replace(r'\.0$', '', regex=True)  # remove trailing .0
         .str.replace(r'\s+', '', regex=True)   # remove all whitespace
         .str.strip()
    )

voucher_lookup['Template ID'] = normalize_id(voucher_lookup['Template ID'])
voucher_lookup['Mechanic Detail'] = voucher_lookup['Mechanic Detail'].astype("string").str.strip()

# Remove bad rows + dedupe
before_dedupe = len(voucher_lookup)
voucher_lookup = (
    voucher_lookup[
        voucher_lookup['Template ID'].notna() &
        (voucher_lookup['Template ID'] != '')
    ]
    .drop_duplicates('Template ID')
)
logger.info(f"Voucher deduplication: {before_dedupe - len(voucher_lookup)} duplicates removed, {len(voucher_lookup)} unique templates remaining")

# ==========================
# Build mapping + apply
# ==========================
template_to_mechanic = dict(zip(
    voucher_lookup['Template ID'],
    voucher_lookup['Mechanic Detail']
))
logger.info(f"Created voucher mapping with {len(template_to_mechanic)} templates")

df_valid['VoucherId'] = normalize_id(df_valid['VoucherId'])
df_valid['VoucherCode'] = df_valid['VoucherId'].map(template_to_mechanic)
logger.info(f"VoucherCode mapped for {df_valid['VoucherCode'].notna().sum()} records")

# Optional: strip spaces from VoucherCode
df_valid['VoucherCode'] = df_valid['VoucherCode'].str.strip()


# %% [markdown]
# **Create & Merge DeviceID, Receiver Phone Pivots**

# %%
# =============================
# DEVICE LEVEL AGGREGATION
# =============================
logger.info("Starting device level aggregation")

# -------- Voucher aggregation --------
logger.debug("Computing voucher aggregation by device")
voucher_df = (
    df_valid
    .dropna(subset=["VoucherCode"])
    .groupby(["RequestedDeviceId", "VoucherCode"])["CustomerOrderCode"]
    .nunique()
    .reset_index()
)

voucher_summary_device = (
    voucher_df
    .groupby("RequestedDeviceId")
    .agg(
        Voucher_Codes=("VoucherCode", lambda x: ", ".join(x.astype(str))),
        Voucher_Counts=("CustomerOrderCode", lambda x: ", ".join(x.astype(str)))
    )
    .reset_index()
)

# -------- Free Shipping aggregation --------
shipping_df = (
    df_valid
    .dropna(subset=["FreeShippingId"])
    .groupby(["RequestedDeviceId", "FreeShippingId"])["CustomerOrderCode"]
    .nunique()
    .reset_index()
)

shipping_summary_device = (
    shipping_df
    .groupby("RequestedDeviceId")
    .agg(
        FreeShippingIds=("FreeShippingId", lambda x: ", ".join(x.astype(str))),
        Count_Per_ShippingID=("CustomerOrderCode", lambda x: ", ".join(x.astype(str)))
    )
    .reset_index()
)

# -------- Main aggregation --------
device_main = (
    df_valid
    .groupby("RequestedDeviceId")
    .agg(
        NumOfAccounts=("CustomerId", "nunique"),
        CustomerID_List=("CustomerId", lambda x: ", ".join(map(str, x.unique()))),
        GOS=("CustomerOrderCode", "nunique"),
        GIS=("OrderQuantity", "sum"),
        GMV=("TotalPrice", "sum"),
        VoucherSubsidy=("VoucherDiscount", "sum"),
        TotalShippingSubsidy=("CartupShipping", "sum")
    )
    .reset_index()
)

# -------- Merge all --------
device_df = (
    device_main
    .merge(voucher_summary_device, on="RequestedDeviceId", how="left")
    .merge(shipping_summary_device, on="RequestedDeviceId", how="left")
)

device_df.fillna("", inplace=True)
device_df.rename(columns={"RequestedDeviceId": "Entity"}, inplace=True)
device_df.insert(0, "EntityType", "DeviceID")
logger.info(f"Device level aggregation completed: {len(device_df)} unique devices")

# =============================
# PHONE LEVEL AGGREGATION
# =============================
logger.info("Starting phone level aggregation")

# -------- Voucher aggregation --------
logger.debug("Computing voucher aggregation by phone")
voucher_df_phone = (
    df_valid
    .dropna(subset=["VoucherCode"])
    .groupby(["Receiver Phone", "VoucherCode"])["CustomerOrderCode"]
    .nunique()
    .reset_index()
)

voucher_summary_phone = (
    voucher_df_phone
    .groupby("Receiver Phone")
    .agg(
        Voucher_Codes=("VoucherCode", lambda x: ", ".join(x.astype(str))),
        Voucher_Counts=("CustomerOrderCode", lambda x: ", ".join(x.astype(str)))
    )
    .reset_index()
)

# -------- Free Shipping aggregation --------
shipping_df_phone = (
    df_valid
    .dropna(subset=["FreeShippingId"])
    .groupby(["Receiver Phone", "FreeShippingId"])["CustomerOrderCode"]
    .nunique()
    .reset_index()
)

shipping_summary_phone = (
    shipping_df_phone
    .groupby("Receiver Phone")
    .agg(
        FreeShippingIds=("FreeShippingId", lambda x: ", ".join(x.astype(str))),
        Count_Per_ShippingID=("CustomerOrderCode", lambda x: ", ".join(x.astype(str)))
    )
    .reset_index()
)

# -------- Main aggregation --------
phone_main = (
    df_valid
    .groupby("Receiver Phone")
    .agg(
        NumOfAccounts=("CustomerId", "nunique"),
        CustomerID_List=("CustomerId", lambda x: ", ".join(map(str, x.unique()))),
        GOS=("CustomerOrderCode", "nunique"),
        GIS=("OrderQuantity", "sum"),
        GMV=("TotalPrice", "sum"),
        VoucherSubsidy=("VoucherDiscount", "sum"),
        TotalShippingSubsidy=("CartupShipping", "sum")
    )
    .reset_index()
)

# -------- Merge all --------
phone_df = (
    phone_main
    .merge(voucher_summary_phone, on="Receiver Phone", how="left")
    .merge(shipping_summary_phone, on="Receiver Phone", how="left")
)

phone_df.fillna("", inplace=True)
phone_df.rename(columns={"Receiver Phone": "Entity"}, inplace=True)
phone_df.insert(0, "EntityType", "Phone")
logger.info(f"Phone level aggregation completed: {len(phone_df)} unique phone numbers")

# =============================
# FINAL MERGE
# =============================
logger.info("Merging device and phone aggregations")

final_merged = pd.concat([device_df, phone_df], ignore_index=True)
logger.info(f"Final merged dataset: {len(final_merged)} total entities (devices + phones)")


# %%
#--------Drop row Entity if blank---------------------
before_entity_filter = len(final_merged)
# 1. Convert empty strings or whitespace-only strings to NaN
final_merged['Entity'] = final_merged['Entity'].replace(r'^\s*$', np.nan, regex=True)

# 2. Drop rows where 'Entity' is NaN
final_merged = final_merged.dropna(subset=['Entity'])
logger.info(f"Dropped blank Entity rows: {before_entity_filter - len(final_merged)} records removed, {len(final_merged)} remaining")

# %% [markdown]
# **Voucher Abuse Detection**

# %%
# ==========================
# Voucher limit map (FAST)
# ==========================
voucher_master = pd.concat([v1, v2], ignore_index=True)[
    ['Mechanic Detail', 'Voucher Per User']
].dropna()

voucher_master['Mechanic Detail'] = voucher_master['Mechanic Detail'].astype(str).str.strip()
voucher_master['Voucher Per User'] = (
    voucher_master['Voucher Per User']
    .astype(str)
    .str.strip()
    .where(lambda x: x.str.isdigit(), '1')
    .astype(int)
)

voucher_limit_map = dict(zip(
    voucher_master['Mechanic Detail'],
    voucher_master['Voucher Per User']
))

# Manual overrides
voucher_limit_map.update({
    "Big Friday Sale FS Bata Voucher 20% on 100 Max 2000/1000 (3-6 PM) 30 Nov": 1,
    "Big Friday Sale FS Lotto Voucher 29% on 100 Max 3000 (3-6 PM) 28 Nov": 1
})

# ==========================
# PRECOMPUTE voucher subsidy per entity + voucher
# ==========================
device_voucher_loss = (
    df_valid
    .dropna(subset=["VoucherCode"])
    .assign(VoucherCode=lambda x: x["VoucherCode"].str.strip())
    .groupby(["RequestedDeviceId", "VoucherCode"])["VoucherDiscount"]
    .sum()
    .to_dict()
)

phone_voucher_loss = (
    df_valid
    .dropna(subset=["VoucherCode"])
    .assign(VoucherCode=lambda x: x["VoucherCode"].str.strip())
    .groupby(["Receiver Phone", "VoucherCode"])["VoucherDiscount"]
    .sum()
    .to_dict()
)

# ==========================
# FAST voucher abuse detection
# ==========================
logger.info("Initializing voucher abuse detection")
def detect_voucher_abuse_fast(row):

    # -------- PRE-CONDITION --------
    if row.get("NumOfAccounts", 0) <= 5:
        return False, "", 0, 0

    if not row["Voucher_Codes"]:
        return False, "", 0, 0

    codes = row["Voucher_Codes"].split(", ")
    counts = list(map(int, row["Voucher_Counts"].split(", ")))

    abused = []
    subsidy_lost = 0

    apg_aph = 0
    winner = 0

    for code, count in zip(codes, counts):

        code_clean = code.strip()

        # APG / APH vouchers
        if code_clean.startswith(("APG", "APH")):
            apg_aph += 1
            if count > 1:
                abused.append(f"{code_clean} [{count}]")
            continue

        # WINNER vouchers
        if "WINNER" in code_clean.upper():
            winner += 1
            if count > 1:
                abused.append(f"{code_clean} [{count}]")
            continue

        # Standard vouchers
        limit = voucher_limit_map.get(code_clean, 1)
        if count > limit:
            abused.append(f"{code_clean} [{count}]")

            if row["EntityType"] == "DeviceID":
                subsidy_lost += device_voucher_loss.get(
                    (row["Entity"], code_clean), 0
                )
            else:
                subsidy_lost += phone_voucher_loss.get(
                    (row["Entity"], code_clean), 0
                )

    # Cross-voucher abuse rules
    if apg_aph >= 2:
        abused.append(f"Multiple APG/APH vouchers [{apg_aph}]")
    if winner >= 2:
        abused.append(f"Multiple WINNER vouchers [{winner}]")

    return (
        bool(abused),
        ", ".join(abused),
        sum(counts),
        subsidy_lost
    )

# ==========================
# APPLY ONCE (FAST)
# ==========================
(
    final_merged["VoucherAbuse"],
    final_merged["Abused_Vouchers"],
    final_merged["TotalVoucherCount"],
    final_merged["VoucherSubsidyLost"]
) = zip(*final_merged.apply(detect_voucher_abuse_fast, axis=1))

# ==========================
# AbusedVoucherCount
# ==========================
final_merged["AbusedVoucherCount"] = (
    final_merged["Abused_Vouchers"]
    .str.findall(r"\[(\d+)\]")
    .apply(lambda x: sum(map(int, x)) if x else 0)
)

# %% [markdown]
# **FreeShipping Abuse Detection**

# %%
# =====================================================
# Load Free Shipping Limits sheet
# =====================================================
logger.info("Loading Free Shipping limits data")
BASE_URL = "https://docs.google.com/spreadsheets/d/1w1B2PavaRA2L2-plQ-nJJ8oK_1ffJw7zyGm4fiy7eWg/export?format=csv&gid="

try:
    free_shipping_limits = pd.read_csv(
        BASE_URL + "340434592",
        header=1,
        usecols=['Template ID', 'Per User']
    )
    logger.info(f"Free Shipping limits loaded: {len(free_shipping_limits)} templates")
except Exception as e:
    logger.error(f"Failed to load Free Shipping limits: {str(e)}")
    raise

# Clean up columns
free_shipping_limits['Template ID'] = free_shipping_limits['Template ID'].astype(str).str.strip()
free_shipping_limits['Per User'] = free_shipping_limits['Per User'].apply(
    lambda x: int(x) if str(x).isdigit() else 10   # fallback default
)

# Build limit lookup
fs_limit_map = dict(zip(
    free_shipping_limits['Template ID'],
    free_shipping_limits['Per User']
))
logger.info(f"Free Shipping limit map created with {len(fs_limit_map)} templates")

# =====================================================
# Pre-aggregate df_valid (CRITICAL for memory safety)
# =====================================================
logger.info("Pre-aggregating free shipping data by device and phone")
df_valid['RequestedDeviceId'] = df_valid['RequestedDeviceId'].astype(str).str.strip()
df_valid['Receiver Phone'] = df_valid['Receiver Phone'].astype(str).str.strip()
df_valid['FreeShippingId'] = df_valid['FreeShippingId'].astype(str).str.strip()

# Device-based subsidy lookup
device_fs_subsidy = (
    df_valid
    .groupby(['RequestedDeviceId', 'FreeShippingId'])['CartupShipping']
    .sum()
    .to_dict()
)
logger.debug(f"Device free shipping subsidy aggregations: {len(device_fs_subsidy)} combinations")

# Phone-based subsidy lookup
phone_fs_subsidy = (
    df_valid
    .groupby(['Receiver Phone', 'FreeShippingId'])['CartupShipping']
    .sum()
    .to_dict()
)
logger.debug(f"Phone free shipping subsidy aggregations: {len(phone_fs_subsidy)} combinations")

# =====================================================
# Shipping Abuse Detector (memory-safe)
# =====================================================
logger.info("Processing shipping abuse detection")
def shipping_abuse_detector(row):

    if (
        pd.isna(row['FreeShippingIds']) or
        pd.isna(row['Count_Per_ShippingID']) or
        row['NumOfAccounts'] <= 5
    ):
        return False, 0

    ids = [x.strip() for x in str(row['FreeShippingIds']).split(',') if x.strip()]
    counts = [int(x) for x in str(row['Count_Per_ShippingID']).split(',') if x.strip().isdigit()]

    if len(ids) != len(counts):
        return False, 0

    abused = False
    subsidy_lost = 0
    entity = str(row['Entity']).strip()

    for fs_id, count in zip(ids, counts):

        if fs_id == '0':   # skip FreeShippingId = 0
            continue

        limit = fs_limit_map.get(fs_id, 10)

        if count > limit:
            abused = True

            if row['EntityType'] == 'DeviceID':
                subsidy_lost += device_fs_subsidy.get((entity, fs_id), 0)
            else:
                subsidy_lost += phone_fs_subsidy.get((entity, fs_id), 0)

    return abused, subsidy_lost

# =====================================================
# Apply Shipping Abuse Detection
# =====================================================
final_merged[['ShippingAbuse', 'ShippingSubsidyLost']] = (
    final_merged
    .apply(lambda r: pd.Series(shipping_abuse_detector(r)), axis=1)
)

# =====================================================
# AbusedShippingIdCount
# =====================================================
def calculate_abused_shipping_count(row):

    if pd.isna(row['FreeShippingIds']) or pd.isna(row['Count_Per_ShippingID']):
        return 0

    ids = [x.strip() for x in str(row['FreeShippingIds']).split(',') if x.strip()]
    counts = [int(x) for x in str(row['Count_Per_ShippingID']).split(',') if x.strip().isdigit()]

    if len(ids) != len(counts):
        return 0

    total = 0
    for fs_id, count in zip(ids, counts):

        if fs_id == '0':  
            continue

        if count > fs_limit_map.get(fs_id, 10):
            total += count

    return total

final_merged['AbusedShippingIdCount'] = final_merged.apply(
    calculate_abused_shipping_count,
    axis=1
)

# %% [markdown]
# **Combine Abuse Types**

# %%
def abuse_type(row):
    if row['VoucherAbuse'] and row['ShippingAbuse']:
        return 'VoucherAbuser; FreeShippingAbuser'
    if row['VoucherAbuse']:
        return 'VoucherAbuser'
    if row['ShippingAbuse']:
        return 'FreeShippingAbuser'
    return None

final_merged['AbuseType'] = final_merged.apply(abuse_type, axis=1)
abuse_merged = final_merged[final_merged['VoucherAbuse'] | final_merged['ShippingAbuse']].copy()

# %% [markdown]
# **Create PhoneFromAddress Column**

# %%
# -----------------------------
# Bangla → English digit map
# -----------------------------
bangla_to_eng = str.maketrans(
    "০১২৩৪৫৬৭৮৯",
    "0123456789"
)

# -----------------------------
# Strict regex (English digits only)
# -----------------------------
phone_pattern = re.compile(
    r'(?<![0-9])(01[0-9]{9})(?![0-9])'
)

def extract_phone_from_address(addr):
    if pd.isna(addr):
        return None

    # normalize Bangla digits → English (not stored)
    addr = str(addr).translate(bangla_to_eng)

    match = phone_pattern.search(addr)
    if not match:
        return None

    phone = match.group(1)

    # OPTIONAL: remove leading 0 (keep as string)
    phone = phone[1:] if phone.startswith("0") else phone

    return phone  # always string

# -----------------------------
# Extract phone
# -----------------------------
df["Phone_From_Address"] = df["Receiver Address"].apply(
    extract_phone_from_address
)

# -----------------------------
# Ensure column is STRING (no .0 issue)
# -----------------------------
df["Phone_From_Address"] = df["Phone_From_Address"].astype("string")


# %% [markdown]
# **Cancellation & Return**

# %%
# =============================
# RETURN LOGIC
# =============================
logger.info("Processing return logic and calculations")

df["is_returned"] = df["IsQC"].notna() & (df["IsFailedDelivery"] == False)
df["is_valid_return"] = (df["IsQC"] == True) & (df["IsFailedDelivery"] == False)
df["is_invalid_return"] = (
    (df["IsQC"] == False) &
    (df["IsFailedDelivery"] == False) &
    df["IsQC"].notna() &
    df["IsFailedDelivery"].notna()
)

df["returned_qty"] = df["OrderQuantity"].where(df["is_returned"], 0)
df["valid_return_qty"] = df["OrderQuantity"].where(df["is_valid_return"], 0)
df["invalid_return_qty"] = df["OrderQuantity"].where(df["is_invalid_return"], 0)
logger.info(f"Return analysis: {df['is_returned'].sum()} returned orders, {df['is_valid_return'].sum()} valid returns, {df['is_invalid_return'].sum()} invalid returns")

df["gmv_valid_return"] = df["TotalPrice"].where(df["is_valid_return"], 0)
df["gmv_invalid_return"] = df["TotalPrice"].where(df["is_invalid_return"], 0)

df["RequestedDeviceId"] = df["RequestedDeviceId"].astype(str).str.strip()


# =============================
# NON-RETURNED DELIVERED LOGIC
# =============================

delivered_mask = (
    (df["CustomerStatus"] == "Delivered") &
    (df["is_returned"] == False)
)

df["nos_flag"] = df["CustomerOrderCode"].where(delivered_mask)
df["nis_qty"] = df["OrderQuantity"].where(delivered_mask, 0)
df["nmv_value"] = df["TotalPrice"].where(delivered_mask, 0)


# =============================
# BUILD ENTITY-LEVEL TABLE
# =============================
logger.info("Building entity-level summary tables")

def build_entity_table(df, entity_col, entity_type, cancel_reasons):

    entity_df = df.copy()
    entity_df["Entity"] = entity_df[entity_col].astype(str).str.strip()

    entity_df = entity_df[
        entity_df["Entity"].notna() &
        ~entity_df["Entity"].str.lower().isin(["nan", "none", ""])
    ]

    entity_df["EntityType"] = entity_type

    # -----------------------------
    # CANCEL LOGIC
    # -----------------------------
    entity_df["CancelCount"] = 0
    customer_cancel_mask = entity_df["CancelReason"].isin(cancel_reasons)

    entity_df.loc[customer_cancel_mask, "CancelCount"] = \
        entity_df.loc[customer_cancel_mask, "OrderQuantity"]

    entity_df["OverallCancelCount"] = 0
    overall_cancel_mask = (
        entity_df["CancelReason"].notna() &
        ~entity_df["CancelReason"].astype(str).str.strip().isin(["", "nan", "none"])
    )

    entity_df.loc[overall_cancel_mask, "OverallCancelCount"] = \
        entity_df.loc[overall_cancel_mask, "OrderQuantity"]

    entity_df["GMV_Cancelled"] = 0.0
    entity_df.loc[customer_cancel_mask, "GMV_Cancelled"] = \
        entity_df.loc[customer_cancel_mask, "TotalPrice"]

    # -----------------------------
    # AGGREGATION
    # -----------------------------
    agg_cols = {
        "OrderQuantity": "sum",
        "CustomerOrderCode": "nunique",
        "TotalPrice": "sum",

        # NEW METRICS
        "nos_flag": "nunique",
        "nis_qty": "sum",
        "nmv_value": "sum",

        "returned_qty": "sum",
        "valid_return_qty": "sum",
        "invalid_return_qty": "sum",
        "gmv_valid_return": "sum",
        "gmv_invalid_return": "sum",
        "CustomerId": lambda x: ", ".join(x.astype(str).unique()),
        "CancelCount": "sum",
        "OverallCancelCount": "sum",
        "GMV_Cancelled": "sum",
        "VoucherDiscount": "sum",
        "CartupShipping": "sum",
        "OrderDate": ["min", "max"],
        "Phone_From_Address": lambda x: ", ".join(x.dropna().astype(str).unique())
    }

    entity_agg = entity_df.groupby(
        ["EntityType", "Entity"], as_index=False
    ).agg(agg_cols)

    # Flatten columns
    entity_agg.columns = [
        "_".join(col).rstrip("_") if isinstance(col, tuple) else col
        for col in entity_agg.columns
    ]

    # Rename
    entity_agg = entity_agg.rename(columns={
        "OrderQuantity_sum": "GIS",
        "CustomerOrderCode_nunique": "GOS",
        "TotalPrice_sum": "GMV",

        "nos_flag_nunique": "NOS",
        "nis_qty_sum": "NIS",
        "nmv_value_sum": "NMV",

        "returned_qty_sum": "total_returned",
        "valid_return_qty_sum": "total_valid_return",
        "invalid_return_qty_sum": "total_invalid_return",
        "gmv_valid_return_sum": "gmv_valid_return",
        "gmv_invalid_return_sum": "gmv_invalid_return",
        "CustomerId_<lambda>": "ListOfCustomerID",
        "Phone_From_Address_<lambda>": "Phone_From_Address",
        "CancelCount_sum": "CancelCount",
        "OverallCancelCount_sum": "OverallCancelCount",
        "GMV_Cancelled_sum": "GMV_Cancelled",
        "VoucherDiscount_sum": "VoucherSubsidy",
        "CartupShipping_sum": "ShippingSubsidy",
        "OrderDate_min": "FirstOrderDate",
        "OrderDate_max": "LastOrderDate"
    })

    # -----------------------------
    # PREPAYMENT SUBSIDY
    # -----------------------------
    prepayment = (
        entity_df.drop_duplicates(subset=["CustomerOrderCode"])
        .groupby(["EntityType", "Entity"], as_index=False)
        .agg(PrepaymentSubsidy=("DiscountAmount", "sum"))
    )

    entity_agg = entity_agg.merge(
        prepayment,
        on=["EntityType", "Entity"],
        how="left"
    )

    entity_agg["PrepaymentSubsidy"] = \
        entity_agg["PrepaymentSubsidy"].fillna(0)

    # -----------------------------
    # TOTAL CUSTOMERS
    # -----------------------------
    total_customers = (
        entity_df.groupby("Entity")["CustomerId"]
        .nunique()
        .rename("NumOfAccounts")
    )

    entity_agg = entity_agg.merge(
        total_customers,
        on="Entity",
        how="left"
    )

    return entity_agg


# =============================
# CANCEL REASONS LIST
# =============================

cancel_reasons = [
    "Customer Cancel the return through CS",
    "Customer Cancellation - Change of Mind",
    "Customer Cancellation - Change/Combine Order",
    "Customer Cancellation - Decided for alternative product",
    "Customer Cancellation - Delivery Time is too long",
    "Customer Cancellation - Duplicate Order",
    "Customer Cancellation - Found Cheaper Elsewhere",
    "Customer Cancellation - Others",
    "Customer Cancellation - Shipping Fee",
    "Customer Cancellation - Voucher Issue",
    "Customer has no cash available",
    "Customer instruct to postpone",
    "Customer not at the location",
    "Customer not reachable",
    "Customer rejected to delivery",
    "Customer requsted cancel through customer care",
    "Duplicate order",
    "I decided to buy a different product",
    "I forgot to use a voucher or had an issue with it",
    "I mistakenly placed a duplicate order",
    "I need to change the delivery address",
    "I no longer want this order",
    "I want to change the payment method",
    "I want to place a new order with more or different items",
    "Internal Cancellation - Change of Payment Method",
    "Internal Cancellation - Change/Combine Order",
    "Internal Cancellation - Customer Unreachable",
    "Internal Cancellation - Duplicate Order",
    "Internal Cancellation - Fake Order",
    "Internal Cancellation - Incomplete/Incorrect Shipping Address",
    "Internal Cancellation - Invalid Order",
    "Internal Cancellation - Others",
    "Internal Cancellation - Voucher Abuser",
    "The shipping cost is too high"
]


# =============================
# BUILD TABLES
# =============================

device_table = build_entity_table(
    df, "RequestedDeviceId", "DeviceID", cancel_reasons
)

phone_table = build_entity_table(
    df, "Receiver Phone", "Phone", cancel_reasons
)

final_entity_table = pd.concat(
    [device_table, phone_table],
    ignore_index=True
)

final_entity_table = final_entity_table.sort_values(
    "total_invalid_return",
    ascending=False
).reset_index(drop=True)


# =============================
# FINAL COLUMN ORDER
# =============================

desired_columns = [
    "EntityType",
    "Entity",
    "NumOfAccounts",
    "ListOfCustomerID",
    "GOS",
    "GIS",
    "GMV",
    "NOS",
    "NIS",
    "NMV",
    "VoucherSubsidy",
    "ShippingSubsidy",
    "PrepaymentSubsidy",
    "CancelCount",
    "OverallCancelCount",
    "GMV_Cancelled",
    "FirstOrderDate",
    "LastOrderDate",
    "total_returned",
    "total_valid_return",
    "total_invalid_return",
    "gmv_valid_return",
    "gmv_invalid_return",
    "Phone_From_Address"
]

final_entity_table = final_entity_table[
    [c for c in desired_columns if c in final_entity_table.columns]
]

final_entity_table = final_entity_table.loc[
    :, ~final_entity_table.columns.duplicated()
]

# %% [markdown]
# **Adding SubsidyAbuse Details in Final Table**

# %%
# =============================
# Add default columns
# =============================
default_cols = {
    "AbusedVoucherCount": 0,
    "VoucherSubsidyLost": 0,
    "AbusedShippingIdCount": 0,
    "ShippingSubsidyLost": 0,
    "AbuseType": "None"
}

for col, default in default_cols.items():
    final_entity_table[col] = default

# =============================
# Merge abuse_merged info
# =============================
merge_cols = [
    "Entity",
    "AbusedVoucherCount",
    "VoucherSubsidyLost",
    "AbusedShippingIdCount",
    "ShippingSubsidyLost",
    "AbuseType"
]

# Only select relevant columns from abuse_merged
abuse_subset = abuse_merged[merge_cols]

# Merge on Entity
final_entity_table = final_entity_table.merge(
    abuse_subset,
    on="Entity",
    how="left",
    suffixes=("", "_from_abuse")
)

# =============================
# Update values from abuse_merged where available
# =============================
for col in ["AbusedVoucherCount", "VoucherSubsidyLost", "AbusedShippingIdCount", "ShippingSubsidyLost", "AbuseType"]:
    final_entity_table[col] = final_entity_table[f"{col}_from_abuse"].combine_first(final_entity_table[col])
    final_entity_table.drop(columns=[f"{col}_from_abuse"], inplace=True)

# %% [markdown]
# **Aggregating Shared CustomerIDs**

# %%
# =========================
# Helper functions
# =========================
def split_clean(val, sep=','):
    if pd.isna(val):
        return []
    return [x.strip() for x in str(val).split(sep) if x.strip()]

def concat_unique(series):
    s = set()
    for v in series.dropna():
        s.update(split_clean(v))
    return ", ".join(sorted(s))

def count_unique_ids(x):
    return len(set(split_clean(x)))

def normalize_abuse_type(val):
    if pd.isna(val) or not str(val).strip():
        return "None"
    parts = [x.strip() for x in re.split(r'[;,]', str(val)) 
             if x.strip() and x.strip() != "None"]
    parts = sorted(set(parts))
    if not parts:
        return "None"
    if len(parts) == 1:
        return parts[0]
    return "VoucherAbuser; FreeShippingAbuser"

# =========================
# Union-Find (NetworkX-style)
# =========================
parent = {}

def find(x):
    if parent[x] != x:
        parent[x] = find(parent[x])
    return parent[x]

def union(a, b):
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[rb] = ra

# =========================
# Step 0: Register all row nodes
# =========================
for idx in final_entity_table.index:
    parent[f"row_{idx}"] = f"row_{idx}"

# =========================
# Step 1: Build connections via ListOfCustomerID
# =========================
for idx, val in final_entity_table['ListOfCustomerID'].items():
    row_node = f"row_{idx}"
    for cid in split_clean(val):
        cid_node = f"cid_{cid}"
        if cid_node not in parent:
            parent[cid_node] = cid_node
        union(row_node, cid_node)

# =========================
# Step 2: Build connections via Phone_From_Address
# =========================
for idx, phone in final_entity_table['Phone_From_Address'].items():
    if pd.notna(phone) and str(phone).strip():
        row_node = f"row_{idx}"
        for p in split_clean(phone):
            phone_node = f"phone_{p}"
            if phone_node not in parent:
                parent[phone_node] = phone_node
            union(row_node, phone_node)

# =========================
# Step 3: Assign deterministic group_id
# =========================
root_to_gid = {}
gid = 0
group_ids = []

for idx in final_entity_table.index:
    root = find(f"row_{idx}")
    if root not in root_to_gid:
        root_to_gid[root] = gid
        gid += 1
    group_ids.append(root_to_gid[root])

final_entity_table['group_id'] = group_ids

# =========================
# Separate Device / Non-device
# =========================
device_rows = final_entity_table[final_entity_table['EntityType'] == 'DeviceID']
non_device_rows = final_entity_table[final_entity_table['EntityType'] != 'DeviceID']

# =========================
# Numeric columns to aggregate
# =========================
numeric_cols = [
    'GOS','GIS','GMV',
    'NOS','NIS','NMV',
    'VoucherSubsidy','ShippingSubsidy','PrepaymentSubsidy',
    'VoucherSubsidyLost','ShippingSubsidyLost',
    'AbusedVoucherCount','AbusedShippingIdCount',
    'CancelCount','OverallCancelCount','GMV_Cancelled',
    'total_returned','total_valid_return','total_invalid_return',
    'gmv_valid_return','gmv_invalid_return'
]

# =========================
# Aggregation for Non-device
# =========================
agg_non_device = {c: 'sum' for c in numeric_cols}
agg_non_device.update({
    'EntityType': 'first',
    'Entity': concat_unique,
    'ListOfCustomerID': concat_unique,
    'Phone_From_Address': concat_unique,
    'FirstOrderDate': 'min',
    'LastOrderDate': 'max',
    'AbuseType': concat_unique
})

merged_non_device = (
    non_device_rows
    .groupby('group_id', sort=False)
    .agg(agg_non_device)
    .reset_index()
)

# =========================
# Aggregation for Device rows (UPDATED)
# =========================
device_info = (
    device_rows
    .groupby('group_id', sort=False)
    .agg({
        'Entity': concat_unique,
        'ListOfCustomerID': concat_unique,
        'AbuseType': concat_unique
    })
    .reset_index()
    .rename(columns={
        'Entity': 'LinkedDevices',
        'ListOfCustomerID': 'DeviceCustomerIDs',
        'AbuseType': 'DeviceAbuseType'
    })
)

# =========================
# Merge Device info with Non-device
# =========================
final_merged = merged_non_device.merge(device_info, on='group_id', how='left')

# =========================
# Merge customer IDs safely
# =========================
final_merged['ListOfCustomerID'] = (
    final_merged[['ListOfCustomerID', 'DeviceCustomerIDs']]
    .fillna('')
    .agg(
        lambda r: ", ".join(
            sorted(set(split_clean(r.iloc[0]) + split_clean(r.iloc[1])))
        ),
        axis=1
    )
)

final_merged['NumOfAccounts'] = final_merged['ListOfCustomerID'].map(count_unique_ids)

# =========================
# Clean LinkedDevices
# =========================
final_merged['LinkedDevices'] = (
    final_merged['LinkedDevices']
    .fillna('')
    .map(lambda x: ", ".join(sorted(set(split_clean(x)))))
)

final_merged['LinkedDeviceCount'] = final_merged['LinkedDevices'].map(count_unique_ids)

# =========================
# Combine & Normalize AbuseType (UPDATED)
# =========================
final_merged['AbuseType'] = (
    final_merged[['AbuseType', 'DeviceAbuseType']]
    .fillna('')
    .agg(
        lambda r: normalize_abuse_type(
            ", ".join(
                sorted(set(
                    split_clean(r.iloc[0]) + split_clean(r.iloc[1])
                ))
            )
        ),
        axis=1
    )
)

# =========================
# Merge Phone_From_Address into Entity
# =========================
def merge_phone_entity(row):
    phones = split_clean(row['Phone_From_Address'])
    entities = split_clean(row['Entity'])
    combined = sorted(set(entities + phones))
    return ", ".join(combined)

final_merged['Entity'] = final_merged.apply(merge_phone_entity, axis=1)

# =========================
# Final cleanup
# =========================
final_entity_table = (
    final_merged
    .drop(columns=['DeviceCustomerIDs', 'DeviceAbuseType'], errors='ignore')
    .drop_duplicates()
    .reset_index(drop=True)
)

# %% [markdown]
# **Aggregating Product Info Table**

# %%
# =====================================================
# STEP 1: Clean df and prepare entity-level data
# =====================================================

df_clean = (
    df.copy()
    .assign(
        Entity=lambda x: x["Receiver Phone"].astype(str).str.strip(),
        EntityType="Phone",
        DeepestCategory=lambda x: x.get("deepestcategory", x["cat1"]),
        OrderDate=lambda x: pd.to_datetime(x["OrderDate"]).dt.date
    )
    .rename(columns={"cat1": "Category"})
)

# Remove invalid entities
df_clean = df_clean[
    df_clean["Entity"].notna() &
    ~df_clean["Entity"].str.lower().isin(["nan", "none", ""])
]

# =====================================================
# STEP 2: Create entity_product_table (Correct)
# =====================================================

entity_product_table = (
    df_clean
    .groupby(
        ["EntityType", "Entity", "OrderDate",
         "Vertical", "Category", "DeepestCategory"],
        as_index=False
    )
    .agg(
        GIS=("OrderQuantity", "sum"),
        GOS=("CustomerOrderCode", "nunique"),
        GMV=("TotalPrice", "sum"),
        ShippingSubsidy=("CartupShipping", "sum"),
        VoucherSubsidy=("VoucherDiscount", "sum")
    )
)

# Ensure numeric safety
numeric_cols = ['GIS', 'GMV', 'ShippingSubsidy', 'VoucherSubsidy', 'GOS']

for col in numeric_cols:
    entity_product_table[col] = (
        pd.to_numeric(entity_product_table[col], errors='coerce')
        .fillna(0)
    )

# =====================================================
# STEP 3: Ensure EntityGroupID exists
# =====================================================

final_entity_table = final_entity_table.reset_index(drop=True)

if 'EntityGroupID' not in final_entity_table.columns:
    final_entity_table['EntityGroupID'] = (
        'EG_' + (final_entity_table.index + 1).astype(str)
    )

# =====================================================
# STEP 4: Build Entity → EntityGroupID Mapping
# =====================================================

entity_mapping = (
    final_entity_table[['EntityGroupID', 'Entity']]
    .assign(Entity=lambda x: x['Entity'].astype(str).str.split(','))
    .explode('Entity')
)

entity_mapping['Entity'] = (
    entity_mapping['Entity']
    .astype(str)
    .str.strip()
)

# Remove empty values
entity_mapping = entity_mapping[
    entity_mapping['Entity'].notna() &
    (entity_mapping['Entity'] != "")
]

# Keep one mapping per Entity
entity_mapping = entity_mapping.drop_duplicates(
    subset=['Entity']
)

# =====================================================
# STEP 5: Merge to Tag EntityGroupID
# =====================================================

entity_group_joined = entity_product_table.merge(
    entity_mapping,
    on='Entity',
    how='left'
)

# =====================================================
# STEP 6: Aggregate to EntityGroup + Date Level
# (Use MAX for GIS to prevent duplication)
# =====================================================

entity_group_product_table = (
    entity_group_joined
    .groupby(
        ['EntityGroupID', 'OrderDate',
         'Vertical', 'Category', 'DeepestCategory'],
        as_index=False
    )
    .agg({
        'GIS': 'sum',
        'GOS': 'sum',
        'GMV': 'sum',
        'ShippingSubsidy': 'sum',
        'VoucherSubsidy': 'sum'
    })
)

print("entity_group_product_table created successfully.")
logger.info("entity_group_product_table created successfully")

# %% [markdown]
# **Shipping Details Table**

# %%

# =====================================================
# STEP 1: Ensure EntityGroupID exists
# =====================================================

final_entity_table = final_entity_table.reset_index(drop=True)

if "EntityGroupID" not in final_entity_table.columns:
    final_entity_table["EntityGroupID"] = (
        "EG_" + (final_entity_table.index + 1).astype(str)
    )

# =====================================================
# STEP 2: Build Clean Entity → EntityGroupID Mapping
# =====================================================

entity_mapping = (
    final_entity_table[["EntityGroupID", "Entity"]]
    .assign(Entity=lambda x: x["Entity"].astype(str).str.split(","))
    .explode("Entity")
)

entity_mapping["Entity"] = (
    entity_mapping["Entity"]
    .astype(str)
    .str.strip()
)

# Remove invalid entities
entity_mapping = entity_mapping[
    entity_mapping["Entity"].notna() &
    (entity_mapping["Entity"] != "")
]

# Keep one mapping per Entity (prevents duplication)
entity_mapping = entity_mapping.drop_duplicates(subset=["Entity"])

# =====================================================
# STEP 3: Prepare Base Shipping Data
# =====================================================

shipping_group_df = (
    df[[
        "Receiver Phone",
        "CustomerOrderCode",
        "TotalPrice",
        "ShippingState",
        "ShippingCity",
        "ShippingZone"
    ]]
    .assign(Entity=df["Receiver Phone"].astype(str).str.strip())
    .merge(entity_mapping, on="Entity", how="inner")
)

# Ensure numeric safety
shipping_group_df["TotalPrice"] = (
    pd.to_numeric(shipping_group_df["TotalPrice"], errors="coerce")
    .fillna(0)
)

# =====================================================
# STEP 4: Aggregate at EntityGroup + Shipping Level
# =====================================================

entity_group_shipping_table = (
    shipping_group_df
    .groupby(
        [
            "EntityGroupID",
            "ShippingState",
            "ShippingCity",
            "ShippingZone"
        ],
        as_index=False
    )
    .agg(
        GOS=("CustomerOrderCode", "nunique"),
        GMV=("TotalPrice", "sum")
    )
)

# =====================================================
# STEP 5: Sort for Readability
# =====================================================

entity_group_shipping_table = (
    entity_group_shipping_table
    .sort_values(
        ["EntityGroupID", "ShippingState", "ShippingCity", "ShippingZone"]
    )
    .reset_index(drop=True)
)

print("entity_group_shipping_table created successfully (clean version).")
logger.info("entity_group_shipping_table created successfully")


# %% [markdown]
# **Labelling Abuses**

# %%
# =============================
# Normalize existing AbuseType (important for reruns)
# =============================
def normalize_abuse(val):
    if pd.isna(val) or val in ["", "None"]:
        return "None"

    parts = sorted(set(
        x.strip()
        for x in str(val).split(";")
        if x.strip() and x.strip() != "None"
    ))

    return "; ".join(parts) if parts else "None"

final_entity_table["AbuseType"] = (
    final_entity_table["AbuseType"]
    .fillna("None")
    .apply(normalize_abuse)
)

# =============================
# Append abuse WITHOUT duplicates
# =============================
def append_abuse(current, new):
    if pd.isna(current) or current in ["None", ""]:
        return new

    parts = {x.strip() for x in str(current).split(";") if x.strip()}
    parts.add(new)

    return "; ".join(sorted(parts))


# =============================
# Apply abuse rules
# =============================

# 1) High Cancel / Return
mask = (
    (final_entity_table["GIS"] > GISthreshold) &
    ((final_entity_table["CancelCount"] + final_entity_table["total_returned"])
     >= CancelReturnThreshold * final_entity_table["GIS"])
)
final_entity_table.loc[mask, "AbuseType"] = (
    final_entity_table.loc[mask, "AbuseType"]
    .apply(lambda x: append_abuse(x, "High Cancel/Return"))
)

# 2) High Invalid Return
mask = (
    (final_entity_table["GIS"] > GISthreshold) &
    (final_entity_table["total_invalid_return"]
     >= InvalidReturnPercent * final_entity_table["GIS"])
)
final_entity_table.loc[mask, "AbuseType"] = (
    final_entity_table.loc[mask, "AbuseType"]
    .apply(lambda x: append_abuse(x, "High Invalid Return"))
)

# 3) Multiple Accounts (Device)
mask = (
    (final_entity_table["EntityType"] == "DeviceID") &
    (final_entity_table["NumOfAccounts"] > 2)
)
final_entity_table.loc[mask, "AbuseType"] = (
    final_entity_table.loc[mask, "AbuseType"]
    .apply(lambda x: append_abuse(x, "Multiple Accounts"))
)

# 4) Multiple Accounts (Phone)
mask = (
    (final_entity_table["EntityType"] == "Phone") &
    (final_entity_table["NumOfAccounts"] > AccountThreshold)
)
final_entity_table.loc[mask, "AbuseType"] = (
    final_entity_table.loc[mask, "AbuseType"]
    .apply(lambda x: append_abuse(x, "Multiple Accounts"))
)

# =============================
# 5) Reseller
# =============================

# -----------------------------
# A) Last 30 days check
# -----------------------------
max_date = entity_group_product_table["OrderDate"].max()
cutoff_date = max_date - pd.Timedelta(days=30)

recent_product = entity_group_product_table[
    entity_group_product_table["OrderDate"] >= cutoff_date
]

recent_resellers = (
    recent_product
    .groupby(["EntityGroupID", "DeepestCategory"], as_index=False)["GIS"]
    .sum()
)

recent_resellers = recent_resellers[
    recent_resellers["GIS"] > DeepCatThreshold
]["EntityGroupID"].unique()


# -----------------------------
# B) Monthly spike (from Oct 2025 onward)
# -----------------------------
start_date = pd.to_datetime("2025-10-01").date()

monthly_product = entity_group_product_table[
    entity_group_product_table["OrderDate"] >= start_date
].copy()

monthly_product["Year"] = pd.to_datetime(monthly_product["OrderDate"]).dt.year
monthly_product["Month"] = pd.to_datetime(monthly_product["OrderDate"]).dt.month

monthly_resellers = (
    monthly_product
    .groupby(
        ["EntityGroupID", "Year", "Month", "DeepestCategory"],
        as_index=False
    )["GIS"]
    .sum()
)

monthly_resellers = monthly_resellers[
    monthly_resellers["GIS"] > DeepCatThreshold
]["EntityGroupID"].unique()



# -----------------------------
# C) Combine both rules
# -----------------------------
reseller_groups = set(recent_resellers) | set(monthly_resellers)
reseller_groups = list(reseller_groups)

# Apply abuse label
mask = final_entity_table["EntityGroupID"].isin(reseller_groups)

final_entity_table.loc[mask, "AbuseType"] = (
    final_entity_table.loc[mask, "AbuseType"]
    .apply(lambda x: append_abuse(x, "Reseller"))
)

# =============================
# FINAL normalization (safety net)
# =============================
final_entity_table["AbuseType"] = final_entity_table["AbuseType"].apply(normalize_abuse)


# %% [markdown]
# **Risk Scoring**

# %%
# =============================
# Define risk weights
# =============================
risk_weights = {
    "Multiple Accounts": 2,
    "High Cancel/Return": 2,
    "High Invalid Return": 2,
    "VoucherAbuser": 3,
    "FreeShippingAbuser": 3,
    "Reseller": 4
}

# =============================
# Compute RiskScore
# =============================
def compute_risk_score(abuse_str):
    if pd.isna(abuse_str) or abuse_str == "None" or abuse_str.strip() == "":
        return 0
    abuses = [a.strip() for a in abuse_str.split(";")]
    return sum(risk_weights.get(a, 0) for a in abuses)

final_entity_table["RiskScore"] = final_entity_table["AbuseType"].apply(compute_risk_score)

# =============================
# Assign base Category
# =============================
final_entity_table["Category"] = np.select(
    [
        final_entity_table["RiskScore"] >= 6,
        final_entity_table["RiskScore"].between(3, 5),
        final_entity_table["RiskScore"].between(0, 2),
    ],
    ["High Risk", "Medium Risk", "Low Risk"],
    default="Low Risk"
)

# =============================
# Loyal Customer logic (FIXED)
# =============================

# Ensure date columns are datetime
final_entity_table["FirstOrderDate"] = pd.to_datetime(final_entity_table["FirstOrderDate"], errors="coerce")
final_entity_table["LastOrderDate"] = pd.to_datetime(final_entity_table["LastOrderDate"], errors="coerce")

avg_gmv = final_entity_table["GMV"].mean()

today = pd.Timestamp.today()
previous_month_start = today.replace(day=1) - pd.DateOffset(months=1)

loyal_mask = (
    (final_entity_table["RiskScore"] == 0) &
    (final_entity_table["GMV"] > avg_gmv) &
    (final_entity_table["LastOrderDate"] >= previous_month_start) &
    ((final_entity_table["LastOrderDate"] - final_entity_table["FirstOrderDate"]) >= pd.Timedelta(days=60))
)

final_entity_table.loc[loyal_mask, "Category"] = "Loyal Customer"
final_entity_table = final_entity_table.drop(columns=["LinkedDevices"])

final_entity_table["FirstOrderDate"] = pd.to_datetime(
    final_entity_table["FirstOrderDate"]
).dt.date

final_entity_table["LastOrderDate"] = pd.to_datetime(
    final_entity_table["LastOrderDate"]
).dt.date


# %% [markdown]
# **Push Product Table**

# %%
# -----------------------------
# Database connection
# -----------------------------
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'admin123'
DB_HOST = '172.16.90.18'
DB_PORT = '5432'
 
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
 
# -----------------------------
# Step 1: Prepare DataFrame
# -----------------------------
EntityGroupDF = entity_group_product_table.copy()

# Normalize column names
EntityGroupDF.columns = (
    EntityGroupDF.columns
        .str.strip()
        .str.replace(" ", "", regex=False)
)

# Ensure OrderDate is string (safe for PostgreSQL)
if "OrderDate" in EntityGroupDF.columns:
    EntityGroupDF["OrderDate"] = pd.to_datetime(
        EntityGroupDF["OrderDate"], errors="coerce"
    )

# -----------------------------
# Required columns
# -----------------------------
string_cols = [
    "EntityGroupID",
    "Vertical",
    "Category",
    "DeepestCategory",
    "OrderDate"
]

numeric_cols = [
    "GIS",
    "GMV",
    "VoucherSubsidy",
    "ShippingSubsidy"
]

# Ensure string columns exist
for col in string_cols:
    if col not in EntityGroupDF.columns:
        EntityGroupDF[col] = "UNKNOWN"

    if col != "OrderDate":
        EntityGroupDF[col] = EntityGroupDF[col].astype(str)

# Ensure numeric columns exist
for col in numeric_cols:
    if col not in EntityGroupDF.columns:
        EntityGroupDF[col] = 0

    EntityGroupDF[col] = (
        pd.to_numeric(EntityGroupDF[col], errors="coerce")
        .fillna(0)
    )

# -----------------------------
# Final column order
# -----------------------------
EntityGroupDF = EntityGroupDF[
    [
        "EntityGroupID",
        "OrderDate",
        "Vertical",
        "Category",
        "DeepestCategory",
        "GIS",
        "GMV",
        "VoucherSubsidy",
        "ShippingSubsidy"
    ]
]



# -----------------------------
# Step 3: Write DataFrame to NEW table
# -----------------------------
EntityGroupDF.to_sql(
    "EntityGroup", #table name
    engine,
    if_exists="replace",
    index=False
)

print("EntityGroup table created successfully")
logger.info("EntityGroup table created successfully")

# %% [markdown]
# **Push Shipping Details Table**

# %%
# -----------------------------
# Database connection
# -----------------------------
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'admin123'
DB_HOST = '172.16.90.18'
DB_PORT = '5432'
 
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
 
# -----------------------------
# Step 1: Prepare shipping DataFrame
# -----------------------------
ShippingDF = entity_group_shipping_table.copy()
 
# Normalize column names
ShippingDF.columns = (
    ShippingDF.columns
        .str.strip()
        .str.replace(" ", "", regex=False)
)
 
# -----------------------------
# Ensure required columns exist and types
# -----------------------------
string_cols = ["EntityGroupID", "ShippingState", "ShippingCity", "ShippingZone"]
numeric_cols = ["GOS", "GMV"]
 
for col in string_cols:
    if col not in ShippingDF.columns:
        ShippingDF[col] = "UNKNOWN"
    ShippingDF[col] = ShippingDF[col].astype(str)
 
for col in numeric_cols:
    if col not in ShippingDF.columns:
        ShippingDF[col] = 0
    ShippingDF[col] = (
        pd.to_numeric(ShippingDF[col], errors="coerce")
          .fillna(0)
    )
 
# -----------------------------
# Final column order
# -----------------------------
ShippingDF = ShippingDF[
    [
        "EntityGroupID",
        "ShippingState",
        "ShippingCity",
        "ShippingZone",
        "GOS",
        "GMV"
    ]
]
 
# -----------------------------
# Step 2: Drop table if exists
# -----------------------------
with engine.begin() as conn:
    conn.execute(
        text('DROP TABLE IF EXISTS "EntityGroupShipping"')
    )
 
print("Existing EntityGroupShipping table dropped (if existed)")
logger.info("Existing EntityGroupShipping table dropped")
 
# -----------------------------
# Step 3: Write DataFrame to database
# -----------------------------
ShippingDF.to_sql(
    "EntityGroupShipping",
    engine,
    if_exists="replace",
    index=False
)
 
print("EntityGroupShipping table created successfully")
logger.info("EntityGroupShipping table created successfully")

# %% [markdown]
# **Rename Columns**

# %%
final_entity_table= final_entity_table.rename(columns={
    "OverallCancelCount": "OverallCancelledItems",
    "total_returned": "TotalReturnedItems",
    "total_valid_return": "ValidReturnedItems",
    "total_invalid_return": "InvalidReturnedItems",
    "CancelCount": "CustomerCancelledItems"
})

# %% [markdown]
# **Push Customer Profiling Table**

# %%
# -----------------------------
# Database connection
# -----------------------------
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'admin123'
DB_HOST = '172.16.90.18'
DB_PORT = '5432'

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# -----------------------------
# Step 1: Prepare DataFrame
# -----------------------------
CustomerProfiling = final_entity_table.copy()

# Remove duplicates by EntityType + Entity
CustomerProfiling = CustomerProfiling.drop_duplicates(
    subset=["EntityType", "Entity"]
)

# -----------------------------
# Numeric columns
# -----------------------------
numeric_cols = [
    "NumOfAccounts", "LinkedDeviceCount", "OverallCancelCount",
    "GOS", "GIS", "GMV", "NOS", "NIS", "NMV", "GMV_Cancelled",
    "VoucherSubsidy", "ShippingSubsidy", "PrepaymentSubsidy",
    "total_returned", "total_valid_return", "total_invalid_return",
    "gmv_valid_return", "gmv_invalid_return",
    "CancelCount",
    "AbusedVoucherCount", "VoucherSubsidyLost",
    "AbusedShippingIdCount", "ShippingSubsidyLost",
    "RiskScore"
]

for col in numeric_cols:
    if col not in CustomerProfiling.columns:
        CustomerProfiling[col] = 0
    CustomerProfiling[col] = (
        pd.to_numeric(CustomerProfiling[col], errors="coerce")
          .fillna(0)
    )

# -----------------------------
# String columns
# -----------------------------
string_cols = [
    "EntityGroupID",
    "EntityType", "Entity", "ListOfCustomerID",
    "AbuseType", "Category"
]

for col in string_cols:
    if col not in CustomerProfiling.columns:
        CustomerProfiling[col] = ""
    CustomerProfiling[col] = CustomerProfiling[col].astype(str)

# -----------------------------
# Rename columns
# -----------------------------
CustomerProfiling = CustomerProfiling.rename(columns={
    "OverallCancelCount": "OverallCancelledItems",
    "total_returned": "TotalReturnedItems",
    "total_valid_return": "ValidReturnedItems",
    "total_invalid_return": "InvalidReturnedItems",
    "CancelCount": "CustomerCancelledItems"
})

# -----------------------------
# Remove duplicate columns (safety)
# -----------------------------
CustomerProfiling = CustomerProfiling.loc[
    :, ~CustomerProfiling.columns.duplicated()
]

# -----------------------------
# Final column order
# -----------------------------
CustomerProfiling = CustomerProfiling[
    [
        "EntityGroupID",
        "EntityType",
        "Entity",
        "ListOfCustomerID",
    
        "NumOfAccounts",
        "LinkedDeviceCount",
        "OverallCancelledItems",

        "GOS",
        "GIS",
        "GMV",
        "NOS",
        "NIS",
        "NMV",
        "GMV_Cancelled",

        "VoucherSubsidy",
        "ShippingSubsidy",
        "PrepaymentSubsidy",

        "TotalReturnedItems",
        "ValidReturnedItems",
        "InvalidReturnedItems",

        "gmv_valid_return",
        "gmv_invalid_return",

        "CustomerCancelledItems",

        "AbusedVoucherCount",
        "VoucherSubsidyLost",
        "AbusedShippingIdCount",
        "ShippingSubsidyLost",

        "AbuseType",
        "RiskScore",
        "Category",

        "FirstOrderDate",
        "LastOrderDate"
    ]
]

# -----------------------------
# Step 2: Drop tables if exist
# -----------------------------
with engine.begin() as conn:
    conn.execute(text('DROP TABLE IF EXISTS "CustomerProfiling_staging"'))
    conn.execute(text('DROP TABLE IF EXISTS "CustomerProfiling"'))

print("Existing CustomerProfiling tables dropped (if existed)")
logger.info("Preparing database: dropping existing CustomerProfiling tables")
logger.debug("Existing tables dropped")

# -----------------------------
# Step 3: Create staging table
# -----------------------------
CustomerProfiling.to_sql(
    "CustomerProfiling_staging",
    engine,
    if_exists="replace",
    index=False
)
logger.info(f"Creating staging table with {len(CustomerProfiling)} records and {len(CustomerProfiling.columns)} columns")
logger.debug("Staging table created")

# -----------------------------
# Step 4: Create final table
# -----------------------------
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE "CustomerProfiling" AS
        SELECT *
        FROM "CustomerProfiling_staging";
    """))

logger.info("Creating final CustomerProfiling table")
print("CustomerProfiling table created successfully")
logger.info("CustomerProfiling table created successfully")
logger.info("="*80)
logger.info("CUSTOMER SEGMENTATION SCRIPT COMPLETED SUCCESSFULLY")
logger.info(f"Final dataset contains {len(CustomerProfiling)} customer entities")
logger.info("="*80)



