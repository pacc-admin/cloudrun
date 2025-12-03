import os
import yaml
import logging
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
    logging.info(f"--- Processing {table_name} ---")
    
    # 1. Get LSN Range
    current_max_lsn = mssql.get_max_lsn()
    start_lsn = state_mgr.get_last_lsn(config['bq_table'])
    
    if start_lsn is None:
        logging.info("Initial load detected.")
        capture_instance = table_name.replace('.', '_')
        start_lsn = mssql.get_min_lsn(capture_instance)
    
    if start_lsn == current_max_lsn:
        logging.info("No new changes.")
        return

    # 2. Fetch Data
    df = mssql.get_changes(table_name, start_lsn, current_max_lsn)
    
    if df.empty:
        logging.info("No rows returned.")
        state_mgr.save_state(config['bq_table'], current_max_lsn)
        return

    # 3. Clean Data
    df.columns = [col.replace('__$', 'cdc_') for col in df.columns]
    for col in df.columns:
        if df[col].dtype == 'object' and len(df) > 0 and isinstance(df[col].iloc[0], bytes):
             df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)

    # 4. Load to BQ
    bq.load_and_merge(df, config['bq_dataset'], config['bq_table'], config['primary_key'])

    # 5. Save State
    state_mgr.save_state(config['bq_table'], current_max_lsn)

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
                logging.error(f"Failed to sync {table_conf['source_table']}: {e}")

if __name__ == "__main__":
    main()