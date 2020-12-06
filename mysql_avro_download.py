import time
from mysql.connector import FieldType
import credential
import re
import datetime
import decimal
from fastavro import writer
from io import BytesIO
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
    print(columns_with_type)

    fields = []
    for colname, coltype in columns_with_type:
        # replace all non-alphanumeric characters with
        name = re.sub("[^0-9a-zA-Z]+", "_", colname.lower())
        coltype = mysql_mapping(FieldType.get_info(coltype))

        fields.append({"name": name, **_avro_switch_type(coltype)})

    counts = 0
    if cur.description:
        rows = cur.fetchall()
        for i in rows:
            counts +=1
        print('rows affected: ' + str(counts))
    else:
        rows = None

    cur.close()

    return fields, table_name, cur, rows, counts



def _create_avro_schema(table_name, fields):
    """constructs the avro schema from table_name and fields"""
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
        records.append(dict(zip(columns, row)))


    # Code below may need to be necessary again:

    # write schema & data to BytesIO object
    file_out = BytesIO()

    writer(file_out, avro_schema, records)

    end_time = time.time()
    print("\nTime to write file_out is :", end_time - start_time)
    return file_out

def mysql_query_to_file(query, table_name):

    fields, table_name, cur, rows, counts = _mysql_select_to_dict(query,table_name)
    avro_schema = _create_avro_schema(table_name, fields)
    file_out = _schema_and_data_to_file(cur, avro_schema, rows)

    return file_out, counts


if __name__ == "__main__":
    mysql_query_to_file('select * from tenjin.load_test_2', 'second_test.json')

