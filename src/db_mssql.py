import pyodbc
import pandas as pd
import os
import logging

class MssqlClient:
    def __init__(self):
        # Lấy thông tin từ biến môi trường
        self.server = os.environ.get("MSSQL_SERVER")
        self.database = os.environ.get("MSSQL_DB")
        self.username = os.environ.get("MSSQL_USER")
        self.password = os.environ.get("MSSQL_PASS")
        
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
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

    # --- HÀM MỚI QUAN TRỌNG: CHUNKING ---
    def get_initial_snapshot_chunks(self, source_table, chunksize=200000):
        """
        Trả về dữ liệu dạng từng cục (Chunk), mặc định 200k dòng/lần.
        """
        sql = f"""
        SELECT 
            *,
            0x00000000000000000000 as __$start_lsn,
            0x00000000000000000000 as __$seqval,
            2 as __$operation,  -- 2 nghĩa là INSERT
            NULL as __$update_mask
        FROM {source_table}
        """
        # Tham số chunksize giúp pandas trả về iterator thay vì dataframe full
        return pd.read_sql(sql, self.conn, chunksize=chunksize)