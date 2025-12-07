import os
import yaml
import logging
import pandas as pd
import gc
from src.db_mssql import MssqlClient
from src.db_bigquery import BigQueryClient
from src.state_manager import StateManager

# Setup Logging
logging.basicConfig(level=logging.INFO)

BQ_PROJECT = os.environ.get("BQ_PROJECT")
STATE_BUCKET = os.environ.get("STATE_BUCKET")
BATCH_SIZE = 50000 

# --- BIẾN QUAN TRỌNG ĐỂ LỌC JOB ---
# Cloud Build truyền vào: 'conn_acc' hoặc 'conn_sales'
FILTER_CONNECTION_ID = os.environ.get("FILTER_CONNECTION_ID")

def load_config():
    with open("config/tables.yaml", "r") as f:
        return yaml.safe_load(f)

# Factory tạo Client
def get_mssql_client(conn_id, all_configs):
    # Tìm config connection tương ứng trong yaml
    connections = all_configs.get('connections', [])
    conn_conf = next((c for c in connections if c['id'] == conn_id), None)
    
    # Nếu không tìm thấy config connection nhưng bảng lại refer tới, ta dùng default
    if not conn_conf:
        logging.warning(f"Connection ID {conn_id} not found in 'connections'. Using default env vars.")
        conn_conf = {} 

    return MssqlClient(conn_conf)

def clean_dataframe(df):
    df.columns = [col.replace('__$', 'cdc_') for col in df.columns]
    
    for col in df.columns:
        if len(df) > 0:
            first_val = df[col].iloc[0]
            if isinstance(first_val, bytes):
                df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)
                df[col] = df[col].astype(str).replace('nan', None)
    
    # Thêm sync_time (DATETIME)
    df['sync_time'] = pd.Timestamp.now()
    return df

def process_table(config, mssql, bq, state_mgr):
    table_name = config['source_table']
    bq_dataset = config['bq_dataset']
    bq_table = config['bq_table']
    pk = config['primary_key']

    logging.info(f"--- Processing {table_name} ---")

    current_max_lsn = mssql.get_max_lsn()
    table_exists = bq.check_table_exists(bq_dataset, bq_table)

    # --- INITIAL LOAD ---
    if not table_exists:
        logging.info(f"🚀 Initial Load detected. Batch size: {BATCH_SIZE}")
        
        chunk_iterator = mssql.get_initial_snapshot_chunks(table_name, chunksize=BATCH_SIZE)
        columns_schema = []
        has_data = False

        for i, chunk_df in enumerate(chunk_iterator):
            has_data = True
            chunk_df = clean_dataframe(chunk_df)
            
            if i == 0:
                columns_schema = chunk_df.columns.tolist()

            bq.load_staging_chunk(chunk_df, bq_dataset, bq_table, is_first_chunk=(i==0))
            
            logging.info(f"✅ Batch {i+1} loaded ({len(chunk_df)} rows).")
            del chunk_df
            gc.collect()

        if has_data:
            logging.info("📦 Executing Merge with Partition...")
            bq.execute_merge(bq_dataset, bq_table, pk, columns_schema)
        else:
            logging.warning("⚠️ Source table is empty.")

    # --- INCREMENTAL LOAD ---
    else:
        start_lsn = state_mgr.get_last_lsn(bq_table)
        
        if start_lsn is None:
            logging.info("State missing. Fallback to Min LSN.")
            capture_instance = table_name.replace('.', '_')
            start_lsn = mssql.get_min_lsn(capture_instance)
            
        if start_lsn == current_max_lsn:
            logging.info("No new changes.")
            return

        logging.info(f"🔄 Syncing changes...")
        df = mssql.get_changes(table_name, start_lsn, current_max_lsn)
        
        if df.empty:
            state_mgr.save_state(bq_table, current_max_lsn)
            return

        df = clean_dataframe(df)
        bq.load_staging_chunk(df, bq_dataset, bq_table, is_first_chunk=True)
        bq.execute_merge(bq_dataset, bq_table, pk, df.columns.tolist())
        
        del df
        gc.collect()

    state_mgr.save_state(bq_table, current_max_lsn)
    logging.info(f"💾 Saved state.")

def main():
    full_config = load_config()
    bq = BigQueryClient(BQ_PROJECT)
    state_mgr = StateManager(STATE_BUCKET)
    clients_cache = {}

    logging.info(f"🚀 Job Started. Filter Mode: {FILTER_CONNECTION_ID if FILTER_CONNECTION_ID else 'ALL'}")

    for table_conf in full_config['tables']:
        # 1. Check Active
        if not table_conf.get('active', True):
            continue

        conn_id = table_conf.get('connection_id')

        # 2. --- LOGIC LỌC JOB ---
        # Nếu Job này được set Filter (VD: conn_acc), mà bảng này thuộc conn_sales -> Bỏ qua
        if FILTER_CONNECTION_ID and conn_id != FILTER_CONNECTION_ID:
            continue
        # ------------------------

        try:
            # Init Connection (nếu chưa có trong cache)
            if conn_id not in clients_cache:
                logging.info(f"🔌 Initializing connection: {conn_id}")
                clients_cache[conn_id] = get_mssql_client(conn_id, full_config)
            
            mssql = clients_cache[conn_id]
            
            # Chạy logic đồng bộ
            process_table(table_conf, mssql, bq, state_mgr)

        except Exception as e:
            logging.error(f"❌ Failed to sync {table_conf.get('source_table')}: {e}", exc_info=True)

if __name__ == "__main__":
    main()