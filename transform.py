import apache_beam as beam
import pyarrow as pa

from apache_beam.io import WriteToParquet
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

# Input Variables
project = "int-peuker-selfmanaged"
region = "europe-west1"
storage = "gs://trainee-data-engineering-challenge-mkaehne"
temp_location = f"{storage}/temp"
job_name = "data-engineering-raw-bq-to-parquet-mkaehne"
dataset = "trainee_data_engineering_challenge_mkaehne"
input_table = "raw"
output_path = f"{storage}/transformation/output"
file_name_suffix = ".parquet"

# create BigQuery Table Reference
table_spec = bigquery.TableReference(
    projectId=project, datasetId=dataset, tableId=input_table
)

# create Schema for Output .parquet File
file_schema = pa.schema(
    [
        ("int64_field_0", pa.int64()),
        ("DATE", pa.date32()),
        ("AIRLINE", pa.string()),
        ("FLIGHT_NUMBER", pa.int32()),
        ("ORIGIN_AIRPORT", pa.string()),
        ("DESTINATION_AIRPORT", pa.string()),
        ("DISTANCE", pa.int32()),
        ("SCHEDULED_DEPARTURE", pa.time32("s")),
        ("SCHEDULED_ARRIVAL", pa.time32("s")),
        ("SCHEDULED_TIME", pa.float32()),
        ("CANCELLED", pa.int32()),
        ("ARRIVAL_DELAY", pa.float32()),
    ]
)

# define Pipeline Options
options = PipelineOptions(
    project=project, region=region, temp_location=temp_location, job_name=job_name
)

with beam.Pipeline(runner="DataflowRunner", options=options) as pipeline:
    write = (
        pipeline
        | "ReadBigQueryTable" >> beam.io.ReadFromBigQuery(table=table_spec)
        | "WriteParquetFileToBucket"
        >> WriteToParquet(
            file_path_prefix=output_path,
            file_name_suffix=file_name_suffix,
            schema=file_schema,
        )
    )