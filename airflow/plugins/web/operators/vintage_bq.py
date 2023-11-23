from typing import Dict, Any, Optional, Sequence, Union
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Added import
import pandas as pd
import requests
import tempfile

class VintageToPostgresOperator(BaseOperator):
    def __init__(
        self,
        api_url: str = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=0HGP7I40XOIJ14WT',
        order_by: str = "",
        postgres_conn_id: str = "postgres_default",  # Replace with your PostgreSQL connection ID
        schema: str = "public",  # Replace with your PostgreSQL schema
        table: str = "vintage_table",  # Replace with your PostgreSQL table
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.order_by = order_by
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table
    

    def execute(self, context: Dict[str, Any]) -> None:
        # download data using requests into a pandas df
        api_url = self.api_url
        self.log.info(f"API url is: {self.api_url}")

        limit = 2000
        offset = 0
        params = {
            '$limit': limit,
            '$offset': offset,
            '$order': self.order_by,  # Specify the field to order by
        }

        # Make a GET request to the API with the headers and parameters
        response = requests.get(api_url, params=params)

        df = pd.DataFrame()

        while True:
            # Make a GET request to the API with the headers and parameters
            response = requests.get(api_url, params=params)

            # Check the response status code
            if response.status_code == 200:
                data = response.json()

                # Process the data and create additional columns
                for k, v in data['Time Series (5min)'].items():
                    v.update({'time_stamp': k })

                df2 = pd.DataFrame(data['Time Series (5min)'].values())
                df2['Symbol'] = data['Meta Data']['2. Symbol']
                df2['Last Refreshed'] = data['Meta Data']['3. Last Refreshed']
                df2['Interval'] = data['Meta Data']['4. Interval']
                df2['Output Size'] = data['Meta Data']['5. Output Size']
                df2['Time Zone'] = data['Meta Data']['6. Time Zone']

                # Remove numbers at the front of column names
                df2.columns = df2.columns.str.replace(r'^\d+\.\s*', '', regex=True)

                df = pd.concat([df, df2], ignore_index=True)
                self.log.info(f"Dataframe currently has {df.shape[0]} columns")

                # If the number of results received is less than the limit, you've reached the end
                if len(data) < limit:
                    break

                # Increment the offset for the next page
                offset += limit
            else:
                self.log.info(f"Request failed with status code: {response.status_code}")
                break

        # Upload DataFrame to PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.upload_to_postgres(postgres_hook, df)

    def upload_to_postgres(self, postgres_hook: PostgresHook, df: pd.DataFrame):
        # Use PostgreSQL hook to insert data into the specified table
        df.to_sql(
            name=self.table,
            con=postgres_hook.get_sqlalchemy_engine(),
            schema=self.schema,
            index=False,
            if_exists='replace',  # You can change this based on your requirements
        )
        self.log.info(f"Loaded data to PostgreSQL table: {self.schema}.{self.table}")

















# from typing import Dict, Any, Optional, Sequence, Union
# from airflow.models import BaseOperator
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# import pandas as pd
# import requests
# import tempfile

# class VintageToGCSBQOperator(BaseOperator):
#     def __init__(
#         self,
#         api_url: str = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=0HGP7I40XOIJ14WT',
#         order_by: str = "",
#         gcp_conn_id: str = "google_cloud_default",
#         api_token: str = "",
#         destination_bucket: Optional[str] = None,
#         delegate_to: Optional[str] = None,
#         impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
#         export_format: str = 'CSV',
#         bq_project_dataset_table: str = "data.csv_table",  # Replace with your BigQuery destination
#         *args,
#         **kwargs
#     ) -> None:
#         super().__init__(*args, **kwargs)
#         self.api_url = api_url
#         self.order_by = order_by
#         self.gcp_conn_id = gcp_conn_id
#         self.api_token = api_token
#         self.destination_bucket = destination_bucket
#         self.delegate_to = delegate_to
#         self.impersonation_chain = impersonation_chain
#         self.export_format = export_format
#         self.bq_project_dataset_table = bq_project_dataset_table

#     def execute(self, context: Dict[str, Any]) -> None:
#         # download data using requests into a pandas df
#         api_url = self.api_url
#         self.log.info(f"API url is: {self.api_url}")

#         limit = 2000
#         offset = 0
#         params = {
#             '$limit': limit,
#             '$offset': offset,
#             '$order': self.order_by,  # Specify the field to order by
#         }

#         # Make a GET request to the API with the headers and parameters
#         response = requests.get(api_url, params=params)

#         df = pd.DataFrame()

#         while True:
#             # Make a GET request to the API with the headers and parameters
#             response = requests.get(api_url, params=params)

#             # Check the response status code
#             if response.status_code == 200:
#                 data = response.json()

#                 # Process the data and create additional columns
#                 for k, v in data['Time Series (5min)'].items():
#                     v.update({'time_stamp': k })

#                 df2 = pd.DataFrame(data['Time Series (5min)'].values())
#                 df2['Symbol'] = data['Meta Data']['2. Symbol']
#                 df2['Last Refreshed'] = data['Meta Data']['3. Last Refreshed']
#                 df2['Interval'] = data['Meta Data']['4. Interval']
#                 df2['Output Size'] = data['Meta Data']['5. Output Size']
#                 df2['Time Zone'] = data['Meta Data']['6. Time Zone']

#                 # Remove numbers at the front of column names
#                 df2.columns = df2.columns.str.replace(r'^\d+\.\s*', '', regex=True)

#                 df = pd.concat([df, df2], ignore_index=True)
#                 self.log.info(f"Dataframe currently has {df.shape[0]} columns")

#                 # If the number of results received is less than the limit, you've reached the end
#                 if len(data) < limit:
#                     break

#                 # Increment the offset for the next page
#                 offset += limit
#             else:
#                 self.log.info(f"Request failed with status code: {response.status_code}")
#                 break

#         # Upload DataFrame to GCS
#         gcs_hook = GCSHook(
#             gcp_conn_id=self.gcp_conn_id,
#             delegate_to=self.delegate_to,
#             impersonation_chain=self.impersonation_chain
#         )
#         gcs_object = self.upload_to_gcs(gcs_hook, df)

#         # Load GCS data into BigQuery
#         self.load_gcs_to_bq(gcs_hook, gcs_object)

#     def upload_to_gcs(self, gcs_hook: GCSHook, df: pd.DataFrame):
#         with tempfile.TemporaryDirectory() as tmpdirname:
#             csv_file = "vintage_data.csv"  # Update the file name
#             df.to_csv(f'{tmpdirname}/{csv_file}', index=False)

#             # Upload to GCS
#             gcs_object = gcs_hook.upload(
#                 bucket_name=self.destination_bucket,
#                 object_name=csv_file,
#                 filename=f'{tmpdirname}/{csv_file}',
#                 mime_type="text/csv",
#                 gzip=False,
#             )
#             self.log.info(f"Loaded CSV to GCS: gs://{self.destination_bucket}/{csv_file}")

#         return gcs_object

#     def load_gcs_to_bq(self, gcs_hook: GCSHook, gcs_object: Dict[str, str]):
#         # Placeholder for BigQuery SQL query
#         bq_sql = f"SELECT * FROM `{self.bq_project_dataset_table}`"

#         # Replace the following line with your specific logic to execute the BigQuery job
#         # Here, I'm using a dummy operator for illustration purposes.
