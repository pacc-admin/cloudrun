import pyodbc
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound
import json
import os
import sys

# ====== LẤY CẤU HÌNH TỪ BIẾN MÔI TRƯỜNG (ENV) ======
# Giúp bảo mật và dễ thay đổi khi chạy trên Cloud Run
try:
    SERVER = os.environ["DB_SERVER"]
    DATABASE = os.environ["DB_NAME"]
    USERNAME = os.environ["DB_USER"]
    PASSWORD = os.environ["DB_PASS"]
    
    BQ_PROJECT = os.environ["BQ_PROJECT"]
    BQ_DATASET = os.environ["BQ_DATASET"]
    
    # Bucket lưu state file
    GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
    
    # Cấu hình bảng cần sync (Format JSON string hoặc set lẻ)
    SOURCE_TABLE = os.environ["SOURCE_TABLE"] # vd: dbo.test_cdc
    TARGET_TABLE = os.environ["TARGET_TABLE"] # vd: test_cdc
    PRIMARY_KEY = os.environ.get("PRIMARY_KEY", "id") # Mặc định là id nếu ko truyền
    
except KeyError as e:
    print(f"❌ Missing environment variable: {e}")
    sys.exit(1)

# Tên file state sẽ tự động theo tên bảng đích
STATE_FILE_NAME = f"state_{TARGET_TABLE}.json"

# ====== CÁC HÀM TIỆN ÍCH ======

def get_db_connection():
    # Chuỗi kết nối cho ODBC Driver 17 (Cài trong Dockerfile)
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={USERNAME};PWD={PASSWORD}"
    )
    return pyodbc.connect(conn_str)

def get_state_from_gcs(bucket_name, file_name):
    try:
        storage_client = storage.Client(project=BQ_PROJECT)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        if blob.exists():
            data = json.loads(blob.download_as_string())
            print(f"📖 Loaded state from gs://{bucket_name}/{file_name}")
            return bytes.fromhex(data.get('last_lsn'))
    except Exception as e:
        print(f"⚠️ Warning reading state: {e}")
    return None

def save_state_to_gcs(bucket_name, file_name, lsn_bytes):
    try:
        storage_client = storage.Client(project=BQ_PROJECT)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps({'last_lsn': lsn_bytes.hex()}))
        print(f"💾 State saved to gs://{bucket_name}/{file_name}")
    except Exception as e:
        print(f"❌ Error saving state: {e}")

# ====== MAIN LOGIC ======

def run_sync():
    print(f"🚀 Starting Sync: {SOURCE_TABLE} -> {BQ_PROJECT}.{BQ_DATASET}.{TARGET_TABLE}")
    
    conn = get_db_connection()
    
    # 1. Lấy Max LSN
    current_max_lsn = pd.read_sql("SELECT sys.fn_cdc_get_max_lsn()", conn).iloc[0, 0]
    
    # 2. Lấy Start LSN
    start_lsn = get_state_from_gcs(GCS_BUCKET_NAME, STATE_FILE_NAME)
    
    if start_lsn is None:
        print("ℹ️ No state found. Fetching Min LSN from DB (First Run)...")
        capture_instance = SOURCE_TABLE.replace('.', '_')
        sql_min = f"SELECT sys.fn_cdc_get_min_lsn('{capture_instance}')"
        start_lsn = pd.read_sql(sql_min, conn).iloc[0, 0]

    # Kiểm tra dữ liệu mới
    if start_lsn == current_max_lsn:
        print("✅ No new changes.")
        return

    # 3. Query CDC Data
    print(f"📥 Fetching changes...")
    cdc_sql = f"""
    SELECT * FROM cdc.fn_cdc_get_all_changes_{SOURCE_TABLE.replace('.', '_')}
    (?, ?, 'all')
    """
    df = pd.read_sql(cdc_sql, conn, params=[start_lsn, current_max_lsn])
    
    if df.empty:
        print("✅ No rows to sync.")
        save_state_to_gcs(GCS_BUCKET_NAME, STATE_FILE_NAME, current_max_lsn)
        return

    # 4. Chuẩn hóa Data
    df.columns = [col.replace('__$', 'cdc_') for col in df.columns]
    for col in df.columns:
        if df[col].dtype == 'object' and len(df) > 0 and isinstance(df[col].iloc[0], bytes):
             df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)

    # Detect PK
    actual_pk = next((c for c in df.columns if c.lower() == PRIMARY_KEY.lower()), None)
    if not actual_pk:
        raise ValueError(f"PK '{PRIMARY_KEY}' not found in columns: {df.columns.tolist()}")

    # 5. Load to Staging
    client = bigquery.Client(project=BQ_PROJECT)
    target_id = f"{BQ_PROJECT}.{BQ_DATASET}.{TARGET_TABLE}"
    staging_id = f"{target_id}_staging"
    
    job = client.load_table_from_dataframe(df, staging_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    print(f"📦 Loaded {len(df)} rows to staging.")

    # 6. Merge / Create
    all_cols = df.columns.tolist()
    table_exists = True
    try: client.get_table(target_id)
    except NotFound: table_exists = False

    final_query = ""
    if not table_exists:
        print("🆕 Creating new table...")
        final_query = f"""
        CREATE OR REPLACE TABLE `{target_id}` AS
        SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY {actual_pk} ORDER BY cdc_start_lsn DESC, cdc_seqval DESC) as rn
            FROM `{staging_id}`
        ) WHERE rn = 1 AND cdc_operation != 1
        """
    else:
        print("🔄 Merging...")
        update_clause = ",\n".join([f"T.{c}=S.{c}" for c in all_cols if c != actual_pk])
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([f"S.{c}" for c in all_cols])
        
        final_query = f"""
        MERGE `{target_id}` T
        USING (
            SELECT * FROM `{staging_id}`
            QUALIFY ROW_NUMBER() OVER(PARTITION BY {actual_pk} ORDER BY cdc_start_lsn DESC, cdc_seqval DESC) = 1
        ) S
        ON T.{actual_pk} = S.{actual_pk}
        WHEN MATCHED AND S.cdc_operation = 1 THEN DELETE
        WHEN MATCHED AND S.cdc_operation != 1 THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED AND S.cdc_operation != 1 THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    
    client.query(final_query).result()
    print("✅ Sync Success.")
    
    # 7. Save State
    save_state_to_gcs(GCS_BUCKET_NAME, STATE_FILE_NAME, current_max_lsn)

if __name__ == "__main__":
    run_sync()