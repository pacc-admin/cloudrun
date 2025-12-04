from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging

class BigQueryClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def load_and_merge(self, df, dataset, table_name, pk):
        target_table_id = f"{self.project_id}.{dataset}.{table_name}"
        staging_table_id = f"{target_table_id}_staging"

        # 1. Load Staging
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded staging: {staging_table_id}")

        # 2. Check Exists
        table_exists = False
        try:
            self.client.get_table(target_table_id)
            table_exists = True
        except NotFound:
            pass

        # 3. Construct Query (Logic merge như cũ của bạn)
        all_columns = df.columns.tolist()
        
        # ... (Copy phần logic tạo query MERGE/CREATE từ code cũ của bạn vào đây) ...
        # Để ngắn gọn, tôi giả định hàm này trả về câu SQL final_query
        
        final_query = self._build_merge_query(target_table_id, staging_table_id, pk, all_columns, table_exists)
        
        self.client.query(final_query).result()
        logging.info(f"Merged into: {target_table_id}")

    def check_table_exists(self, dataset, table_name):
        table_id = f"{self.project_id}.{dataset}.{table_name}"
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False

    def _build_merge_query(self, target, staging, pk, cols, exists):
            # Tìm tên cột PK chính xác (để tránh lỗi hoa/thường)
            actual_pk = next((c for c in cols if c.lower() == pk.lower()), pk)
            
            # --- SỬA Ở ĐÂY: Thêm CAST(... AS STRING) ---
            # Điều này giúp tránh lỗi nếu PK bị nhận diện là Float
            partition_clause = f"PARTITION BY CAST({actual_pk} AS STRING)"
            # -------------------------------------------

            if not exists:
                return f"""
                CREATE OR REPLACE TABLE `{target}` AS
                SELECT * EXCEPT(rn)
                FROM (
                    SELECT *, 
                        ROW_NUMBER() OVER({partition_clause} ORDER BY cdc_start_lsn DESC, cdc_seqval DESC) as rn
                    FROM `{staging}`
                ) WHERE rn = 1 AND cdc_operation != 1
                """
            else:
                # Logic MERGE
                update_list = [f"T.{c}=S.{c}" for c in cols if c != actual_pk]
                update_clause = ", ".join(update_list)
                insert_cols = ", ".join(cols)
                insert_vals = ", ".join([f"S.{c}" for c in cols])
                
                return f"""
                MERGE `{target}` T
                USING (
                    SELECT * FROM `{staging}`
                    QUALIFY ROW_NUMBER() OVER({partition_clause} ORDER BY cdc_start_lsn DESC, cdc_seqval DESC) = 1
                ) S
                ON T.{actual_pk} = S.{actual_pk}
                WHEN MATCHED AND S.cdc_operation = 1 THEN DELETE
                WHEN MATCHED AND S.cdc_operation != 1 THEN UPDATE SET {update_clause}
                WHEN NOT MATCHED AND S.cdc_operation != 1 THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                """