import mysql.connector as connection
import pandas as pd
import csv
import csvkit

def mysql_db_connection():

    sql_conn_flag = True
    while sql_conn_flag:
        choice1 = int(input("\nSQL Operataions\n1.Able to connect to MYSQL \n2.Create script of tables Orders,Returns and Regions\n3.Create tables Orders,Returns and Regions in MYSQL"
                            "\n4. Do a bulk load Orders,Returns and Regions dataset in MYSQL\n\nEnter Your Choice :"))
        if choice1   == 1:
            connect_db()
        elif choice1 == 2:
            create_table_script()
        elif choice1 == 3:
            create_mysql_table()
        elif choice1 == 4:
            load_mysql_table()
        else:
            print("Return to Main Operations")
            sql_conn_flag = False


def connect_db():
    try:
        mydb = connection.connect(host="localhost",database= "projectdb", user="devuser", passwd="Logitech1234#", use_pure=True)
        show_query = "SHOW DATABASES"
        print("we are in connect_db")
        cursor = mydb.cursor()  # create a cursor to execute queries
        cursor.execute(show_query)
        print("able to connect to MYSQL DATABASE")
        return(mydb)
    except Exception as e:
            mydb.close()
            return (str(e))

def create_table_script():
    try:
        import os
        os.popen('"csvsql --dialect mysql --snifflimit 100000 "D:\\Study\\Data Science\\Python\\ineuron\\Data_Set\\Superstore_USA_Orders.csv" > "D:\\Study\\Data Science\\Python\\ineuron\\Data_Set\\Superstore_USA_Orders.sql"')
        os.popen('"csvsql --dialect mysql --snifflimit 100000 "D:\\Study\\Data Science\\Python\\ineuron\\Data_Set\\Superstore_USA_Regions.csv" > "D:\\Study\\Data Science\\Python\\ineuron\\Data_Set\\Superstore_USA_Regions.sql"')
        os.popen('"csvsql --dialect mysql --snifflimit 100000 "D:\\Study\\Data Science\\Python\\ineuron\\Data_Set\\Superstore_USA_Returns.csv" > "D:\\Study\\Data Science\\Python\\ineuron\\Data_Set\\Superstore_USA_Returns.sql"')
    except Exception as e:
            print(f"Error in create_table_Script {e}")


def create_mysql_table():
    try:
        mydb = connection.connect(host="localhost", user="devuser", passwd="Logitech1234#", use_pure=True)
        cursor = mydb.cursor()  # create a cursor to execute queries
        use_db_command = "USE projectdb"
        cursor.execute(use_db_command)

        # Open and read the file as a single buffer
        fd = open('D:\Study\Data Science\Python\ineuron\Data_Set\Superstore_USA_Orders.sql', 'r')
        sqlFile = fd.read()
        print(sqlFile)
        fd.close()
        # In case of multiple sql commands in file we can apply below approach
        # table_create_command = sqlFile.split(';')
        # for command in table_create_command:
        #     print(command)
        #     cursor.execute(command)

        # In case of single sql commands in file we can apply below approach
        table_create_command = sqlFile
        print(table_create_command)
        cursor.execute(table_create_command)
        print("connection success")

        # Open and read the file for ReturnsTable
        fd = open('D:\Study\Data Science\Python\ineuron\Data_Set\Superstore_USA_Returns.sql', 'r')
        sqlFile = fd.read()
        print(sqlFile)
        fd.close()
        # In case of multiple sql commands in file we can apply below approach
        # table_create_command = sqlFile.split(';')
        # for command in table_create_command:
        #     print(command)
        #     cursor.execute(command)

        # In case of single sql commands in file we can apply below approach
        table_create_command = sqlFile
        print(table_create_command)
        cursor.execute(table_create_command)

        # Open and read the file for Regions Table
        fd = open('D:\Study\Data Science\Python\ineuron\Data_Set\Superstore_USA_Regions.sql', 'r')
        sqlFile = fd.read()
        print(sqlFile)
        fd.close()
        # In case of multiple sql commands in file we can apply below approach
        # table_create_command = sqlFile.split(';')
        # for command in table_create_command:
        #     print(command)
        #     cursor.execute(command)

        # In case of single sql commands in file we can apply below approach
        table_create_command = sqlFile
        print(table_create_command)
        cursor.execute(table_create_command)


    except Exception as e:
            print(str(e))
            mydb.close()

def load_mysql_table():
    try:
        mydb = connection.connect(host="localhost", user="devuser", passwd="Logitech1234#", use_pure=True)
        cursor = mydb.cursor()  # create a cursor to execute queries
        use_db_command = "USE projectdb"
        cursor.execute(use_db_command)
        load_data_query1 = "load data  infile 'D://Study//Data Science//Python//ineuron//Data_Set//Superstore_USA_Orders.csv' into table superstore_usa_orders fields terminated by ','  IGNORE 1 ROWS;"
        cursor.execute(load_data_query1)
        # load_data_query2 = "load data  infile 'D://Study//Data Science//Python//ineuron//Data_Set//Superstore_USA_Regions.csv' into table superstore_usa_regions fields terminated by ','  Enclosed by '" "'  IGNORE 1 ROWS;"
        # cursor.execute(load_data_query2)
        # load_data_query3 = "load data  infile 'D://Study//Data Science//Python//ineuron//Data_Set//Superstore_USA_Returns.csv' into table Superstore_USA_Returns fields terminated by ','  Enclosed by '" "'  IGNORE 1 ROWS;"
        # cursor.execute(load_data_query3)
        print("upload success")
        mydb.commit()
    except Exception as e:
            mydb.close()
            print(f"upload fail : {e}")
