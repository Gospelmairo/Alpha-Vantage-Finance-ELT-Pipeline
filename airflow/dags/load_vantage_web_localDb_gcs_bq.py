import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from web.operators.vintage_bq import VintageToPostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator  # Added import
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
API_TOKEN = os.environ.get("API_TOKEN")
OBJECT = "vintage_data"
DATASET = "Alt_engin"
FILE_FORMAT = "PARQUET"

# Database configuration
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# PostgreSQL connection string
POSTGRES_CONN_STRING = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


with DAG(
    dag_id="Load-Vintage-Data-From-Web-To-Postgres-GCS-To-BQ",
    description="Job to move data from Eviction website to Google Cloud Storage and then transfer from GCS to BigQuery, and finally create a data model using dbt",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 21 * * *",
    max_active_runs=1,
    catchup=True,
    tags=["Vintage-Website-to-GCS-Bucket-to-BQ"],
) as dag:
    start = EmptyOperator(task_id="start")

    download_web_to_postgres = VintageToPostgresOperator(  # Create a new operator for downloading to PostgreSQL
        task_id='download_to_postgres',
        api_url='https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=*******',
        order_by='open',
        postgres_conn_id='postgres_default',  # Replace with your PostgreSQL connection ID
        schema='public',  # Replace with your PostgreSQL schema
        table='vintage_table',  # Replace with your PostgreSQL table
    )

    upload_postgres_to_gcs = PostgresToGCSOperator(  # Create a new operator for uploading from PostgreSQL to GCS
        task_id='upload_postgres_to_gcs',
        sql='SELECT * FROM vintage_table',  # Replace with your SQL query
        postgres_conn_id='postgres_default',  # Replace with your PostgreSQL connection ID
        bucket=BUCKET,
        filename='vintage_data.json'
    )

    load_gcs_to_bgquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bgquery",
        bucket=BUCKET,
        source_objects=['vintage_data.json'],
        destination_project_dataset_table=f"{DATASET}.vintage_data_table",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        source_format ="NEWLINE_DELIMITED_JSON",
    )

    end = EmptyOperator(task_id="end")

    start >> download_web_to_postgres >> upload_postgres_to_gcs >> load_gcs_to_bgquery >> end
























# import os
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from web.operators.vintage_bq import VintageToGCSBQOperator  # Updated import
# from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
# from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# DEFAULT_ARGS = {
#     "owner": "airflow",
#     "start_date": datetime(2022, 1, 1),
#     "email": [os.getenv("ALERT_EMAIL", "")],
#     "email_on_failure": True,
#     "email_on_retry": False,
#     "retries": 2,
#     "retry_delay": timedelta(minutes=1),
# }

# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")
# API_TOKEN = os.environ.get("API_TOKEN")
# OBJECT = "vintage_data"
# DATASET = "Alt_engin"
# FILE_FORMAT = "PARQUET"

# # Database configuration
# PG_HOST = os.environ.get("PG_HOST")
# PG_PORT = os.environ.get("PG_PORT")
# PG_DATABASE = os.environ.get("PG_DATABASE")
# PG_USER = os.environ.get("PG_USER")
# PG_PASSWORD = os.environ.get("PG_PASSWORD")

# # PostgreSQL connection string
# POSTGRES_CONN_STRING = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


# with DAG(
#     dag_id="Load-Vintage-Data-From-Web-To-GCS-To-BQ",
#     description="Job to move data from Eviction website to Google Cloud Storage and then transfer from GCS to BigQuery, and finally create a data model using dbt",
#     default_args=DEFAULT_ARGS,
#     schedule_interval="0 21 * * *",
#     max_active_runs=1,
#     catchup=True,
#     tags=["Vintage-Website-to-GCS-Bucket-to-BQ"],
# ) as dag:
#     start = EmptyOperator(task_id="start")

#     # Use the new VintageToGCSBQOperator
#     download_web_to_gcs_bq = VintageToGCSBQOperator(
#         task_id='download_to_gcs',
#         api_url='https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=0HGP7I40XOIJ14WT',
#         order_by='open',
#         gcp_conn_id='google_cloud_default',
#         destination_bucket=BUCKET,

#     )

#     load_gcs_to_bgquery =  GCSToBigQueryOperator(
#         task_id = "load_gcs_to_bgquery",
#         bucket=f"{BUCKET}", #BUCKET
#         source_objects=['vintage_data.csv'], # SOURCE OBJECT
#         destination_project_dataset_table=f"{DATASET}.vintage_data_table", # `nyc.green_dataset_data` i.e table name
#         autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
#         write_disposition="WRITE_TRUNCATE", # command to update table from the  latest (or last row) row number upon every job run or task run
#     )

#     end = EmptyOperator(task_id="end")

#     start >> download_web_to_gcs_bq >> load_gcs_to_bgquery >> end







