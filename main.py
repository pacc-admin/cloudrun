import os
import yaml
import logging
import pandas as pd
from src.db_mssql import MssqlClient
from src.db_bigquery import BigQueryClient
from src.state_manager import StateManager

# Setup Logging
logging.basicConfig(level=logging.INFO)

BQ_PROJECT = os.environ.get("BQ_PROJECT")
STATE_BUCKET = os.environ.get("STATE_BUCKET")

def load_config():
    with open("config/tables.yaml", "r") as f:
        return yaml.safe_load(f)

def clean_dataframe(df):
    # Đổi tên cột __$ thành cdc_
    df.columns = [col.replace('__$', 'cdc_') for col in df.columns]
    
    # Duyệt qua các cột để xử lý dữ liệu Binary -> Hex String
    for col in df.columns:
        # Lấy mẫu dữ liệu dòng đầu tiên (nếu có)
        if len(df) > 0:
            first_val = df[col].iloc[0]
            
            # Chỉ convert nếu là bytes (varbinary/binary)
            if isinstance(first_val, bytes):
                # Apply hex() cho toàn bộ cột, xử lý cả giá trị Null/None
                df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)
                
                # Ép về string để chắc chắn (tránh mixed types)
                df[col] = df[col].astype(str).replace('nan', None)

    # Lưu ý: KHÔNG convert datetime thành string ở đây.
    # Để nguyên object datetime để BigQueryClient._build_schema nhận diện được.
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
        logging.info(f"🚀 Initial Load detected for {table_name}.")
        
        chunk_iterator = mssql.get_initial_snapshot_chunks(table_name, chunksize=100000)
        columns_schema = []
        has_data = False

        for i, chunk_df in enumerate(chunk_iterator):
            has_data = True
            chunk_df = clean_dataframe(chunk_df)
            
            if i == 0:
                columns_schema = chunk_df.columns.tolist()

            # Load Staging (Schema sẽ được tự động build và force trong hàm này)
            bq.load_staging_chunk(chunk_df, bq_dataset, bq_table, is_first_chunk=(i==0))
            
            logging.info(f"✅ Batch {i+1} loaded.")
            del chunk_df

        if has_data:
            logging.info("📦 Executing Merge...")
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
        
        # Load changes vào Staging (Vẫn dùng hàm load cũ, nó sẽ tự apply schema chuẩn)
        bq.load_staging_chunk(df, bq_dataset, bq_table, is_first_chunk=True)
        bq.execute_merge(bq_dataset, bq_table, pk, df.columns.tolist())

    state_mgr.save_state(bq_table, current_max_lsn)
    logging.info(f"💾 Saved state.")

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
                logging.error(f"❌ Failed to sync {table_conf['source_table']}: {e}", exc_info=True)

if __name__ == "__main__":
    main()