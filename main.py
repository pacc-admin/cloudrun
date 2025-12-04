import os
import yaml
import logging
import pandas as pd
from src.db_mssql import MssqlClient
from src.db_bigquery import BigQueryClient
from src.state_manager import StateManager

# Setup Logging
logging.basicConfig(level=logging.INFO)

# Config
BQ_PROJECT = os.environ.get("BQ_PROJECT")
STATE_BUCKET = os.environ.get("STATE_BUCKET")

def load_config():
    with open("config/tables.yaml", "r") as f:
        return yaml.safe_load(f)

# Hàm phụ trợ để clean data cho gọn code chính
def clean_dataframe(df):
    df.columns = [col.replace('__$', 'cdc_') for col in df.columns]
    for col in df.columns:
        if df[col].dtype == 'object' and len(df) > 0:
             first_val = df[col].iloc[0]
             if isinstance(first_val, bytes):
                 df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)
    return df

def process_table(config, mssql, bq, state_mgr):
    table_name = config['source_table']
    bq_dataset = config['bq_dataset']
    bq_table = config['bq_table']
    pk = config['primary_key']

    logging.info(f"--- Processing {table_name} ---")

    # 1. Get LSN
    current_max_lsn = mssql.get_max_lsn()
    table_exists = bq.check_table_exists(bq_dataset, bq_table)

    # --- LOGIC INITIAL LOAD (CHUNKING) ---
    if not table_exists:
        logging.info(f"🚀 Initial Load detected for {table_name}. Starting Batch Processing...")
        
        # Lấy Iterator (batch 200k dòng)
        chunk_iterator = mssql.get_initial_snapshot_chunks(table_name, chunksize=100000)
        
        columns_schema = []
        total_rows = 0
        has_data = False

        for i, chunk_df in enumerate(chunk_iterator):
            has_data = True
            # Clean data
            chunk_df = clean_dataframe(chunk_df)
            
            # Lưu schema cột từ chunk đầu tiên để dùng cho bước Merge sau cùng
            if i == 0:
                columns_schema = chunk_df.columns.tolist()

            # Load vào Staging (Chunk 0 thì Truncate, Chunk > 0 thì Append)
            bq.load_staging_chunk(chunk_df, bq_dataset, bq_table, is_first_chunk=(i==0))
            
            rows_count = len(chunk_df)
            total_rows += rows_count
            logging.info(f"✅ Processed Batch {i+1}: {rows_count} rows (Total: {total_rows})")
            
            # Giải phóng RAM ngay lập tức
            del chunk_df

        if has_data:
            logging.info("📦 All batches loaded. Executing Merge...")
            bq.execute_merge(bq_dataset, bq_table, pk, columns_schema)
        else:
            logging.warning("⚠️ Source table is empty. No data loaded.")

    # --- LOGIC INCREMENTAL LOAD (CDC) ---
    else:
        start_lsn = state_mgr.get_last_lsn(bq_table)
        
        if start_lsn is None:
            logging.info("State missing. Fallback to Min LSN.")
            capture_instance = table_name.replace('.', '_')
            start_lsn = mssql.get_min_lsn(capture_instance)
            
        if start_lsn == current_max_lsn:
            logging.info("No new changes.")
            return

        logging.info(f"🔄 Incremental Sync from {start_lsn.hex()} to {current_max_lsn.hex()}")
        df = mssql.get_changes(table_name, start_lsn, current_max_lsn)
        
        if df.empty:
            logging.info("No rows returned.")
            state_mgr.save_state(bq_table, current_max_lsn)
            return

        # Clean & Load (CDC thường ít data nên load 1 cục luôn cũng đc)
        df = clean_dataframe(df)
        
        # Tái sử dụng hàm load_staging_chunk với is_first_chunk=True để xóa staging cũ
        bq.load_staging_chunk(df, bq_dataset, bq_table, is_first_chunk=True)
        bq.execute_merge(bq_dataset, bq_table, pk, df.columns.tolist())

    # 5. Save State
    state_mgr.save_state(bq_table, current_max_lsn)
    logging.info(f"💾 Saved state LSN: {current_max_lsn.hex()}")

def main():
    configs = load_config()
    mssql = MssqlClient()
    bq = BigQueryClient(BQ_PROJECT)
    state_mgr = StateManager(STATE_BUCKET)

    for table_conf in configs['tables']:
        if table_conf.get('active', True):
            try:
                process_table(table_conf, mssql, bq, state_mgr)
            except Exception as e:
                logging.error(f"❌ Failed to sync {table_conf['source_table']}: {e}")

if __name__ == "__main__":
    main()