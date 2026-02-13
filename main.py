from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from io import StringIO

def load_gcs_csv_to_bq(bucket_name, file_path, project_id, dataset_id, table_id):
    """
    Read CSV from GCS bucket and load to BigQuery table.
    
    Args:
        bucket_name: GCS bucket name
        file_path: Path to CSV file in bucket
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
    """
    # Initialize GCS client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Read CSV from GCS
    csv_content = blob.download_as_string().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    
    # Display columns
    print(f"Columns found: {df.columns.tolist()}")
    print(f"Rows: {len(df)}")
    
    # Initialize BigQuery client
    bq_client = bigquery.Client(project=project_id)
    table_id_full = f"{project_id}.{dataset_id}.{table_id}"
    
    # Load to BigQuery
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    load_job = bq_client.load_table_from_dataframe(
        df, table_id_full, job_config=job_config
    )
    load_job.result()
    
    print(f"Loaded {load_job.output_rows} rows to {table_id_full}")

# Usage example
if __name__ == "__main__":
    load_gcs_csv_to_bq(
        bucket_name="your-bucket-name",
        file_path="path/to/file.csv",
        project_id="your-project-id",
        dataset_id="your-dataset",
        table_id="your-table"
    )