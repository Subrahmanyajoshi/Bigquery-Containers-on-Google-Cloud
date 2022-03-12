import os
import argparse

from google.cloud import bigquery, logging
from google.cloud.exceptions import NotFound

logging_client = logging.Client()
logger = logging_client.logger("projects/text-analysis-323506/logs/containers-log")

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
        # logger.log_text(f"Created dataset {self.bq_client.project}.{dataset.dataset_id}")

    def create_table(self):
        """ Creates bigquery table """
        table = bigquery.Table(self.table_id, schema=self.table_schema)
        table = self.bq_client.create_table(table)
        # logger.log_text(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    def insert(self):
        """ Insert values into the specified table """

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
        # logger.log_text("Inserted {} rows".format(self.dest_table.num_rows))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs-path', type=str, required=True,
                        help="Gcs path of the csv file beginning with 'gs://'")
    args = parser.parse_args()

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/key.json'
    bq_pusher = BigqueryPusher(args.gcs_path)
    bq_pusher.insert()
