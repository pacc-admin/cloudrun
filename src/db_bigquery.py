from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging
import pandas as pd

class BigQueryClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    # --- HÀM 1: Load từng chunk vào Staging ---
    def load_staging_chunk(self, df, dataset, table_name, is_first_chunk=False):
        target_table_id = f"{self.project_id}.{dataset}.{table_name}"
        staging_table_id = f"{target_table_id}_staging"

        disposition = "WRITE_TRUNCATE" if is_first_chunk else "WRITE_APPEND"

        # Build schema (bao gồm cả cột sync_time mới thêm)
        schema_config = self._build_schema(df)

        job_config = bigquery.LoadJobConfig(
            write_disposition=disposition,
            schema=schema_config, 
            autodetect=True
        )
        
        job = self.client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded chunk to {staging_table_id} ({len(df)} rows) | Mode: {disposition}")

    # --- HÀM 2: Merge ---
    def execute_merge(self, dataset, table_name, pk, columns):
        target_table_id = f"{self.project_id}.{dataset}.{table_name}"
        staging_table_id = f"{target_table_id}_staging"
        
        table_exists = self.check_table_exists(dataset, table_name)
        final_query = self._build_merge_query(target_table_id, staging_table_id, pk, columns, table_exists)
        self.client.query(final_query).result()
        logging.info(f"Merged staging into: {target_table_id}")

    # --- HÀM 3: Build Schema ---
    def _build_schema(self, df):
        schema = []
        
        # 1. Map cứng cho các cột hệ thống để đảm bảo đúng Type
        system_map = {
            "cdc_start_lsn": "STRING",
            "cdc_end_lsn":   "STRING",
            "cdc_seqval":    "STRING",
            "cdc_update_mask": "STRING",
            "cdc_operation": "INT64",
            "sync_time":     "DATETIME"  # <--- THÊM MỚI: Ép kiểu Datetime cho cột sync
        }

        for col in df.columns:
            if col in system_map:
                schema.append(bigquery.SchemaField(col, system_map[col]))
            
            # Tự động detect các cột Datetime khác của bảng gốc
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                schema.append(bigquery.SchemaField(col, "DATETIME"))
        
        return schema

    def check_table_exists(self, dataset, table_name):
        table_id = f"{self.project_id}.{dataset}.{table_name}"
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False

    def _build_merge_query(self, target, staging, pk, cols, exists):
        actual_pk = next((c for c in cols if c.lower() == pk.lower()), pk)
        
        # Partition clause dùng cho Window Function (Deduplicate)
        dedup_partition = f"PARTITION BY CAST({actual_pk} AS STRING)"

        if not exists:
            # --- TẠO BẢNG MỚI VỚI PARTITION ---
            # Lưu ý: sync_time phải tồn tại trong cols (đã được add từ pandas)
            return f"""
            CREATE OR REPLACE TABLE `{target}`
            PARTITION BY DATE(sync_time)  -- <--- PARTITION THEO NGÀY
            OPTIONS(
                require_partition_filter = FALSE
            )
            AS
            SELECT * EXCEPT(rn)
            FROM (
                SELECT *, 
                       ROW_NUMBER() OVER({dedup_partition} ORDER BY cdc_start_lsn DESC, cdc_seqval DESC) as rn
                FROM `{staging}`
            ) WHERE rn = 1 AND cdc_operation != 1
            """
        else:
             # --- MERGE (INCREMENTAL) ---
             # Logic này sẽ tự động update sync_time mới nhất cho các dòng có thay đổi
             update_list = [f"T.{c}=S.{c}" for c in cols if c != actual_pk]
             update_clause = ", ".join(update_list)
             insert_cols = ", ".join(cols)
             insert_vals = ", ".join([f"S.{c}" for c in cols])
             
             return f"""
             MERGE `{target}` T
             USING (
                SELECT * FROM `{staging}`
                QUALIFY ROW_NUMBER() OVER({dedup_partition} ORDER BY cdc_start_lsn DESC, cdc_seqval DESC) = 1
             ) S
             ON T.{actual_pk} = S.{actual_pk}
             WHEN MATCHED AND S.cdc_operation = 1 THEN DELETE
             WHEN MATCHED AND S.cdc_operation != 1 THEN UPDATE SET {update_clause}
             WHEN NOT MATCHED AND S.cdc_operation != 1 THEN INSERT ({insert_cols}) VALUES ({insert_vals})
             """