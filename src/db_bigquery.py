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
        # 1. XỬ LÝ PRIMARY KEY (Hỗ trợ Composite Key)
        # Nếu pk là list thì giữ nguyên, nếu là string thì đưa vào list
        if isinstance(pk, list):
            pk_list_raw = pk
        else:
            pk_list_raw = [pk]

        # Map tên cột PK trong config sang tên cột thực tế trong DataFrame (xử lý case-insensitive)
        # Ví dụ: Config ghi "id", Dataframe là "ID" -> lấy "ID"
        actual_pk_list = []
        for key in pk_list_raw:
            found_col = next((c for c in cols if c.lower() == key.lower()), key)
            actual_pk_list.append(found_col)

        # 2. XÂY DỰNG CÁC MỆNH ĐỀ SQL
        
        # Mệnh đề Partition cho Deduplicate (ROW_NUMBER)
        # VD: PARTITION BY CAST(Col1 AS STRING), CAST(Col2 AS STRING)
        dedup_cols = ", ".join([f"CAST({k} AS STRING)" for k in actual_pk_list])
        dedup_partition = f"PARTITION BY {dedup_cols}"

        # Mệnh đề JOIN cho MERGE
        # VD: T.Col1 = S.Col1 AND T.Col2 = S.Col2
        join_conditions = " AND ".join([f"T.{k} = S.{k}" for k in actual_pk_list])

        if not exists:
            # --- TẠO BẢNG MỚI VỚI PARTITION ---
            return f"""
            CREATE OR REPLACE TABLE `{target}`
            PARTITION BY DATE(sync_time)  -- Vẫn giữ Partition theo ngày sync
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
             # Update tất cả các cột NGOẠI TRỪ các cột nằm trong Primary Key
             update_list = [f"T.{c}=S.{c}" for c in cols if c not in actual_pk_list]
             update_clause = ", ".join(update_list)
             
             insert_cols = ", ".join(cols)
             insert_vals = ", ".join([f"S.{c}" for c in cols])
             
             return f"""
             MERGE `{target}` T
             USING (
                SELECT * FROM `{staging}`
                QUALIFY ROW_NUMBER() OVER({dedup_partition} ORDER BY cdc_start_lsn DESC, cdc_seqval DESC) = 1
             ) S
             ON {join_conditions}
             WHEN MATCHED AND S.cdc_operation = 1 THEN DELETE
             WHEN MATCHED AND S.cdc_operation != 1 THEN UPDATE SET {update_clause}
             WHEN NOT MATCHED AND S.cdc_operation != 1 THEN INSERT ({insert_cols}) VALUES ({insert_vals})
             """