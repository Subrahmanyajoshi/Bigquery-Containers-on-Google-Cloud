import argparse

from google.cloud import bigquery, logging


class BigqueryPusher(object):

    def __init__(self, gcs_path: str):
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
        self.dest_table = self.bq_client.get_table(self.table_id)
        self.logging_client = logging.Client()
        self.log_name = "containers-log"
        self.logger = self.logging_client.logger(self.log_name)

    def insert(self):
        self.logger.log_text("Number of rows in the table before inserting: {}".format(self.dest_table.num_rows))

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
        self.logger.log_text("Number of rows in the table after inserting: {}".format(self.dest_table.num_rows))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs-path', type=str, required=True,
                        help="Gcs path of the csv file beginning with 'gs://'")
    args = parser.parse_args()

    bq_pusher = BigqueryPusher(args.gcs_path)
    bq_pusher.insert()
