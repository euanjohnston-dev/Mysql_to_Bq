import mysql.connector

default_mysql_conn = mysql.connector.connect(
    host="HOST_NAME",
    user="USER",
    password="PASSWORD"
)