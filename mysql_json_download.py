import json
import credential


def mysql_connection():

    # comment out of not using the credential file
    default_mysql_conn = credential.default_mysql_conn

    return default_mysql_conn


def _mysql_select_to_dict(query):
    ''' query Mysql and return json '''

    default_mysql_conn =  mysql_connection()
    cur = default_mysql_conn.cursor(dictionary=True)


    print('Executing: \n \n' + query)
    cur.execute(query)
    if cur.description:
        rows = cur.fetchall()
        counts = 0
        for i in rows:
            counts +=1
        print('rows affected: ' + str(counts))
    else:
        rows = None




    cur.close()
    return rows


def _rows_to_json_file(rows, filename):
    with open(filename, 'w') as f:
        for row in rows:
            json.dump(row, f, default=str)
            f.write('\n')

    return filename


def mysql_query_to_file(query, filename):
    json_data = _mysql_select_to_dict(query)
    filename = _rows_to_json_file(json_data, filename)
    print(filename)
    return filename


if __name__ == "__main__":
    mysql_query_to_file('select 10 as col2', 'second_test.json')






