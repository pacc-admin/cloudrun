from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging

class BigQueryClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    # --- HÀM CŨ (Giữ lại để tương thích nếu cần, hoặc dùng logic mới bên dưới) ---
    def load_and_merge(self, df, dataset, table_name, pk):
        self.load_staging_chunk(df, dataset, table_name, is_first_chunk=True)
        self.execute_merge(dataset, table_name, pk, df.columns.tolist())

    # --- HÀM MỚI 1: Load từng chunk vào Staging ---
    def load_staging_chunk(self, df, dataset, table_name, is_first_chunk=False):
        target_table_id = f"{self.project_id}.{dataset}.{table_name}"
        staging_table_id = f"{target_table_id}_staging"

        # Chunk đầu tiên thì XÓA CŨ (WRITE_TRUNCATE)
        # Các chunk sau thì GHI NỐI (WRITE_APPEND)
        disposition = "WRITE_TRUNCATE" if is_first_chunk else "WRITE_APPEND"

        job_config = bigquery.LoadJobConfig(write_disposition=disposition)
        
        job = self.client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded chunk to {staging_table_id} ({len(df)} rows) | Mode: {disposition}")

    # --- HÀM MỚI 2: Thực hiện Merge sau khi đã load hết chunk ---
    def execute_merge(self, dataset, table_name, pk, columns):
        target_table_id = f"{self.project_id}.{dataset}.{table_name}"
        staging_table_id = f"{target_table_id}_staging"
        
        # Check bảng đích tồn tại ko
        table_exists = self.check_table_exists(dataset, table_name)
            
        final_query = self._build_merge_query(target_table_id, staging_table_id, pk, columns, table_exists)
        self.client.query(final_query).result()
        logging.info(f"Merged staging into: {target_table_id}")

    def check_table_exists(self, dataset, table_name):
        table_id = f"{self.project_id}.{dataset}.{table_name}"
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False

    def _build_merge_query(self, target, staging, pk, cols, exists):
        actual_pk = next((c for c in cols if c.lower() == pk.lower()), pk)
        
        # CAST AS STRING để tránh lỗi Partition Float
        partition_clause = f"PARTITION BY CAST({actual_pk} AS STRING)"

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