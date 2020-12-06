import json
import time
from mysql.connector import FieldType
import credential
import re
import datetime
import decimal
from fastavro import writer
from io import BytesIO
from google.cloud import storage
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="creds.json"

def mysql_connection():

    # comment out of not using the credential file
    default_mysql_conn = credential.default_mysql_conn

    return default_mysql_conn

# Need to consider potential further data types here (currently only set for the 3 in the dummy table

def _avro_switch_type(i):
    """replaces Python type with avro type"""
    switcher = {
        int: {"type": ["int", "null"]}, #int
        str: {"type": ["string", "null"]},
        datetime.date: {"type": ["null", {"type": "int", "logicalType": "date"}]},
        datetime.datetime: {"type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        datetime.time: {"type": ["null", {"type": "long", "logicalType": "time-micros"}]},
        float: {"type": ["float", "null"]},
        bool: {"type": ["bool", "null"]},
        decimal.Decimal: {"type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 10}]}
    }

    return switcher.get(i, "STRING")


# Mapping of mysql column type to python column type
def mysql_mapping(coltype):

    if coltype == 'LONG':
        coltype = int
    if coltype == 'VAR_STRING':
        coltype = str
    if coltype == 'DATETIME':
        coltype = datetime.datetime

    return coltype


def _mysql_select_to_dict(query,table_name):
    ''' query Mysql and return json '''

    default_mysql_conn =  mysql_connection()
    cur = default_mysql_conn.cursor() #dictionary=True

    print('Executing: \n \n' + query)
    cur.execute(query)

    columns_with_type = [(col[0], col[1]) for col in cur.description]
    fields = []
    for colname, coltype in columns_with_type:
        # replace all non-alphanumeric characters with
        name = re.sub("[^0-9a-zA-Z]+", "_", colname.lower())

        coltype = mysql_mapping(FieldType.get_info(coltype))

        fields.append({"name": name, **_avro_switch_type(coltype)})

    if cur.description:
        rows = cur.fetchall()
        counts = 0
        for i in rows:
            counts +=1
        print('rows affected: ' + str(counts))
    else:
        rows = None

    cur.close()

    return fields, table_name, cur, rows



def _create_avro_schema(table_name, fields):
    """constructs the avro schema from table_name and fields"""
    print(fields)
    avro_schema = {"namespace": "pnc",
                   "type": "record",
                   "name": table_name,
                   "fields": fields
                   }
    # print("AVRO schema: ", avro_schema)
    return avro_schema


def _schema_and_data_to_file(cur, avro_schema,rows):
    """writes avro schema and selected data to avro BytesIO object file_out"""
    start_time = time.time()
    # parse data into list of dicts
    records = []
    columns = [col[0] for col in cur.description]
    # replace all non-alphanumeric characters with "_"
    columns = [re.sub("[^0-9a-zA-Z]+", "_", col.lower()) for col in columns]
    for row in rows:
        print(row)
        records.append(dict(zip(columns, row)))

    # write schema & data to BytesIO object
    file_out = BytesIO()
    writer(file_out, avro_schema, records)
    end_time = time.time()
    print("\nTime to write file_out is :", end_time - start_time)
    return file_out


def _cloud_storage_upload(file_out, bucket, filename):
    """uploads file to Google Cloud storage"""
    print(file_out)
    print(filename)
    print(bucket)
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(filename)
    blob.upload_from_string(file_out.getvalue(), content_type='application/avro')


def _cloud_storage_to_bigquery(bucket, database, table_name, filename):
    """uploads file from cloud storage to bigquery"""
    client = bigquery.Client()
    #table_id = "acrm-analytics-poc.data_exploration_increment.{}_{}".format(database, table_name)
    table_id = "{}.{}".format(database, table_name)


    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # WRITE_APPEND for adding data
        source_format=bigquery.SourceFormat.AVRO,
        use_avro_logical_types=True
    )
    # TODO: parameter bucket
    uri = "gs://{}/{}".format(bucket, filename)
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # API request

    start_time = time.time()
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

    end_time = time.time()
    print("\nTime to load to BigQuery is :", end_time - start_time)


def mysql_query_to_file(query, local_filename,table_name):

    fields, table_name, cur, rows = _mysql_select_to_dict(query,table_name)
    avro_schema = _create_avro_schema(table_name, fields)
    file_out = _schema_and_data_to_file(cur, avro_schema, rows)
    #table_name = table_name.replace(".", "_")

    database = 'get-data-team.mysql_load_test'
    table_name = 'load_test_1'
    bucket = 'datateam_bucket'

    upload_file_name = "{}_{}.avro".format(database, table_name)
    _cloud_storage_upload(file_out, bucket, upload_file_name)
    _cloud_storage_to_bigquery(bucket, database, table_name, upload_file_name)


    #filename = _rows_to_json_file(json_data, filename)
    print(file_out)
    return file_out


if __name__ == "__main__":
    mysql_query_to_file('select * from tenjin.load_test_2', 'second_test.json')





