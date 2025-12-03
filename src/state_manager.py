from google.cloud import storage
import json
import logging

class StateManager:
    def __init__(self, bucket_name, folder="cdc_states"):
        self.bucket_name = bucket_name
        self.folder = folder
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def get_last_lsn(self, table_name):
        blob_name = f"{self.folder}/state_{table_name}.json"
        blob = self.bucket.blob(blob_name)
        
        if blob.exists():
            try:
                data = json.loads(blob.download_as_string())
                logging.info(f"Loaded LSN for {table_name}: {data.get('last_lsn')}")
                return bytes.fromhex(data.get('last_lsn'))
            except Exception as e:
                logging.warning(f"Error reading state file: {e}")
        return None

    def save_state(self, table_name, lsn_bytes):
        blob_name = f"{self.folder}/state_{table_name}.json"
        blob = self.bucket.blob(blob_name)
        state_data = {'last_lsn': lsn_bytes.hex()}
        blob.upload_from_string(json.dumps(state_data), content_type='application/json')
        logging.info(f"Saved LSN for {table_name}")