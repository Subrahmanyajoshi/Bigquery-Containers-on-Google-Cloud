import os
import argparse

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

client = google.cloud.logging.Client(project='text-analysis-323506')
handler = CloudLoggingHandler(client)
cloud_logger = logging.getLogger('cloudLogger')
cloud_logger.setLevel(logging.INFO)
cloud_logger.addHandler(handler)


class BigqueryPusher(object):

    def __init__(self, gcs_path: str):
        self.database_id = 'text-analysis-323506.iris_dataset'
        self.table_id = 'text-analysis-323506.iris_dataset.iris_table'
        self.table_schema = [
            bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("SepalLengthCm", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("SepalWidthCm", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("PetalLengthCm", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("PetalWidthCm", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Species", "STRING", mode="REQUIRED")
        ]
        self.gcs_path = gcs_path
        self.bq_client = bigquery.Client()

        try:
            self.bq_client.get_dataset(self.database_id)
        except NotFound:
            self.create_database()

        try:
            self.dest_table = self.bq_client.get_table(self.table_id)
        except NotFound:
            self.create_table()
            self.dest_table = self.bq_client.get_table(self.table_id)

    def create_database(self):
        """ Creates bigquery database """
        dataset = bigquery.Dataset(self.database_id)
        dataset.location = "us-east1"
        dataset = self.bq_client.create_dataset(dataset, timeout=30)
        cloud_logger.info(f"Created dataset {self.bq_client.project}.{dataset.dataset_id}")

    def create_table(self):
        """ Creates bigquery table """
        table = bigquery.Table(self.table_id, schema=self.table_schema)
        table = self.bq_client.create_table(table)
        cloud_logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    def insert(self):
        """ Insert values into the specified table """

        cloud_logger.info("Total number of rows in the table before inserting {} ".format(self.dest_table.num_rows))

        # Bigquery load job configuration
        job_config = bigquery.LoadJobConfig(
            schema=self.table_schema,
            skip_leading_rows=1,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
            # WRITE_TRUNCATE replaces existing data
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        # Bigquery load job object
        load_job = self.bq_client.load_table_from_uri(
            self.gcs_path, self.table_id, job_config=job_config,
        )

        # Waiting for inserting to complete
        load_job.result()
        self.dest_table = self.bq_client.get_table(self.table_id)
        cloud_logger.info("Total number of rows in the table after inserting {} ".format(self.dest_table.num_rows))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs-path', type=str, required=True,
                        help="Gcs path of the csv file beginning with 'gs://'")
    args = parser.parse_args()

    key_path = '/opt/key.json'
    if not os.path.exists(key_path):
        raise ValueError(f"Google Cloud service account key path {key_path} doesn't exist")

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    bq_pusher = BigqueryPusher(args.gcs_path)
    bq_pusher.insert()
