from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging
import pandas as pd  # Cần import pandas để check kiểu dữ liệu

class BigQueryClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    # --- HÀM 1: Load từng chunk vào Staging ---
    def load_staging_chunk(self, df, dataset, table_name, is_first_chunk=False):
        target_table_id = f"{self.project_id}.{dataset}.{table_name}"
        staging_table_id = f"{target_table_id}_staging"

        disposition = "WRITE_TRUNCATE" if is_first_chunk else "WRITE_APPEND"

        # --- LOGIC MỚI: XÂY DỰNG SCHEMA ĐỂ ÉP KIỂU ---
        # Việc này giúp tránh lỗi:
        # 1. CDC column (String) bị nhận thành Int64/Float
        # 2. Datetime column bị nhận thành Int64 (do dữ liệu rỗng hoặc timestamp)
        schema_config = self._build_schema(df)

        job_config = bigquery.LoadJobConfig(
            write_disposition=disposition,
            schema=schema_config, # Ép kiểu các cột đã định nghĩa
            autodetect=True       # Các cột còn lại (String, Float chuẩn) để BQ tự lo
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

    # --- HÀM PHỤ TRỢ: Build Schema Dynamic ---
    def _build_schema(self, df):
        schema = []
        
        # 1. Định nghĩa cứng cho các cột CDC
        # Giúp đồng nhất giữa Initial Load (Snapshot) và Incremental (CDC)
        cdc_map = {
            "cdc_start_lsn": "STRING",
            "cdc_end_lsn":   "STRING",
            "cdc_seqval":    "STRING",
            "cdc_update_mask": "STRING",
            "cdc_operation": "INT64"
        }

        for col in df.columns:
            # A. Nếu là cột CDC -> Ép theo map
            if col in cdc_map:
                schema.append(bigquery.SchemaField(col, cdc_map[col]))
            
            # B. Nếu là cột Datetime -> Ép về DATETIME hoặc TIMESTAMP
            # Pandas thường nhận diện là datetime64[ns]
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                # Dùng DATETIME an toàn hơn TIMESTAMP nếu source không có Timezone
                schema.append(bigquery.SchemaField(col, "DATETIME"))
                
            # C. Các trường hợp khác (Int, Float, String business)
            # Để autodetect lo, hoặc bạn có thể mở rộng logic tại đây nếu muốn
        
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
        partition_clause = f"PARTITION BY CAST({actual_pk} AS STRING)"

        if not exists:
            # Lưu ý: Khi create table AS SELECT, BQ sẽ lấy schema của bảng Staging
            # Do bảng Staging đã được ép kiểu đúng ở bước Load, bảng Target sẽ đúng theo.
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