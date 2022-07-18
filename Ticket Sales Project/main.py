import mysql.connector
from mysql.connector import errorcode
import csv
def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user='username',
        password='password',
        host='127.0.0.1',
        port='3306',
        database='ticket_sales')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection

def create_table(connection):
    cursor = connection.cursor()
    table_description = (
    "  CREATE TABLE `ticket_sale` ("
    "  `ticket_id` int NOT NULL,"
    "  `trans_date` date,"
    "  `event_id` int,"
    "  `event_name` varchar(50),"
    "  `event_date` date,"
    "  `event_type` varchar(10),"
    "  `event_city` varchar(20),"
    "  `customer_id` int,"
    "  `price` decimal,"
    "  `num_tickets` int"
    ")")
    try:
        print("Creating table")
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("Table ticket_sale Created")
    cursor.close()


def load_third_party(connection,file_path_csv):
    cursor = connection.cursor()
    query = ("INSERT INTO ticket_sale "
               "VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)")
    with open(file_path_csv,newline='') as f:
        reader = csv.reader(f)
        row_count = 0
        for row in reader:
            cursor.execute(query,(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
            row_count = row_count + 1
    connection.commit()
    print(f"Inserted {row_count} rows")
    cursor.close()

def query_popular_tickets(connection):
# Get the most popular ticket in the past month
    sql_statement = "SELECT event_name FROM ticket_sale ORDER BY num_tickets DESC LIMIT 3"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    print('Here are the most popular tickets:')
    for rec in records:
        print(f"- {rec[0]}")


create_table(get_db_connection())
file_path_csv = '<<localfilepath>>/data/third_party_sales_1.csv'
load_third_party(get_db_connection(),file_path_csv)
query_popular_tickets(get_db_connection())