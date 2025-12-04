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
STATE_BUCKET = os.environ.get("STATE_BUCKET") # Bucket lưu state file

def load_config():
    with open("config/tables.yaml", "r") as f:
        return yaml.safe_load(f)

def process_table(config, mssql, bq, state_mgr):
    table_name = config['source_table']
    bq_dataset = config['bq_dataset']
    bq_table = config['bq_table']
    pk = config['primary_key']

    logging.info(f"--- Processing {table_name} ---")
    
    # 1. Lấy Max LSN hiện tại (Để đánh dấu mốc sau khi load xong)
    current_max_lsn = mssql.get_max_lsn()

    # 2. Kiểm tra bảng trên BigQuery đã tồn tại chưa?
    # Lưu ý: Cần đảm bảo file src/db_bigquery.py đã có hàm check_table_exists
    table_exists = bq.check_table_exists(bq_dataset, bq_table)

    df = pd.DataFrame()

    if not table_exists:
        # --- TRƯỜNG HỢP 1: INITIAL LOAD (Load toàn bộ) ---
        logging.info(f"🚀 Table {bq_table} not found. Fetching FULL SNAPSHOT from source...")
        
        # Lưu ý: Cần đảm bảo file src/db_mssql.py đã có hàm get_initial_snapshot
        df = mssql.get_initial_snapshot(table_name)
    
    else:
        # --- TRƯỜNG HỢP 2: INCREMENTAL LOAD (Chạy CDC) ---
        start_lsn = state_mgr.get_last_lsn(bq_table)
        
        if start_lsn is None:
            # Bảng có nhưng mất file state -> Lấy từ Min LSN của hệ thống
            logging.warning(f"State file missing for {bq_table}. Fallback to Min LSN.")
            capture_instance = table_name.replace('.', '_')
            start_lsn = mssql.get_min_lsn(capture_instance)
        
        if start_lsn == current_max_lsn:
            logging.info("✅ No new changes found on SQL Server.")
            return

        logging.info(f"🔄 Fetching changes from {start_lsn.hex()} to {current_max_lsn.hex()}")
        df = mssql.get_changes(table_name, start_lsn, current_max_lsn)

    # Kiểm tra nếu DataFrame rỗng
    if df.empty:
        logging.info("⚠️ No rows returned from SQL Server.")
        # Vẫn lưu state để lần sau không phải check lại đoạn này
        state_mgr.save_state(bq_table, current_max_lsn)
        return

    # 3. Chuẩn hóa dữ liệu
    # Đổi tên cột hệ thống __$ thành cdc_ (Vì BQ không hỗ trợ ký tự $)
    df.columns = [col.replace('__$', 'cdc_') for col in df.columns]

    # Chuyển đổi dữ liệu Binary (như LSN) sang Hex String để lưu được vào BigQuery
    for col in df.columns:
        if df[col].dtype == 'object' and len(df) > 0:
             # Lấy mẫu dòng đầu tiên để check kiểu dữ liệu
             first_val = df[col].iloc[0]
             if isinstance(first_val, bytes):
                 df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)

    # 4. Load & Merge vào BigQuery
    logging.info(f"📦 Loading {len(df)} rows to BigQuery...")
    bq.load_and_merge(df, bq_dataset, bq_table, pk)

    # 5. Lưu State mới
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
                # Không raise lỗi để nó tiếp tục chạy các bảng khác (nếu có)

if __name__ == "__main__":
    main()