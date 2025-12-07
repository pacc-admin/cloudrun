import pyodbc
import pandas as pd
import os
import logging

class MssqlClient:
    def __init__(self, config=None):
        # Mặc định prefix là MSSQL nếu không khai báo trong yaml
        prefix = "MSSQL"
        if config and 'env_prefix' in config:
            prefix = config['env_prefix']

        # Logic ưu tiên:
        # 1. Tìm biến có Prefix (Ví dụ: MSSQL_PROD_SERVER)
        # 2. Nếu không thấy, tìm biến mặc định (MSSQL_SERVER) - Cái này khớp với Cloud Build của bạn
        
        self.server = os.environ.get(f"{prefix}_SERVER") or os.environ.get("MSSQL_SERVER")
        self.database = os.environ.get(f"{prefix}_DB") or os.environ.get("MSSQL_DB")
        self.username = os.environ.get(f"{prefix}_USER") or os.environ.get("MSSQL_USER")
        self.password = os.environ.get(f"{prefix}_PASS") or os.environ.get("MSSQL_PASS")
        
        # Validate
        if not self.server or not self.password:
            # Ghi log rõ ràng để debug nếu quên set env
            logging.error(f"Missing Env Vars. Prefix attempted: {prefix}")
            raise ValueError(f"Environment variables for MSSQL connection are missing.")
        
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
        logging.info(f"🔌 Connecting to MSSQL: {self.server} -> {self.database}")
        self.conn = pyodbc.connect(conn_str)

    def get_max_lsn(self):
        sql = "SELECT sys.fn_cdc_get_max_lsn()"
        return pd.read_sql(sql, self.conn).iloc[0, 0]

    def get_min_lsn(self, capture_instance):
        sql = f"SELECT sys.fn_cdc_get_min_lsn('{capture_instance}')"
        return pd.read_sql(sql, self.conn).iloc[0, 0]

    def get_changes(self, source_table, start_lsn, end_lsn):
        capture_instance = source_table.replace('.', '_')
        sql = f"""
        SELECT * FROM cdc.fn_cdc_get_all_changes_{capture_instance}
        (?, ?, 'all')
        """
        return pd.read_sql(sql, self.conn, params=[start_lsn, end_lsn])

    def get_initial_snapshot_chunks(self, source_table, chunksize=50000):
        sql = f"""
        SELECT 
            *,
            CAST(0x00 AS BINARY(10)) as __$start_lsn,
            CAST(0x00 AS BINARY(10)) as __$seqval,
            2 as __$operation,
            CAST(0x00 AS VARBINARY(128)) as __$update_mask
        FROM {source_table}
        """
        return pd.read_sql(sql, self.conn, chunksize=chunksize)