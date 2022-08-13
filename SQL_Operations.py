# # Task 2 - Superstore USA
# 1. Load data in sql using primary and foreign key and Pandas
# 2. Table creation should be done automatically
# 3. Find out how many return we have received and what are their ID's
# 4. Join order and return data in sql and pandas
# 5. Find out unique customers
# 6. In how many regions we are selling and who is the Manager of corresponding region
# 7. Find out how many different shipment mode we have and what is the percentage usabliity of all the shipment with respect to dataset
# 8. create a new column and try to find out a difference between order date and shipment date
# 9. Based on 8, find out order id for which shipment durtion is > 10 days
# 10. Try to find out a list of returned order where shipment duration was more than 15 days and who is the manager for it
# 11. groupby region and find out which region is more profitable
# 12. Find out which country we are giving more discount
# 13. provide list of unique post code
# 14. which customer segment is more profitable
# 15. 10th most loss making product
# 16. top 10 product with highest margin


import pandas as pd
import mysql.connector as connection
def sql_db_operations():
    sql_oper_flag = True
    while sql_oper_flag:
        choice2 = int(input(
            "\nSQL Operataions"
            "\n1. Load and Print dataset "
            "\n2. How Many returns received and what are their ID's"
            "\n3. Join Order and Return File and print it"
            "\n4. Print Unique Customer Lists"
            "\n5. How Many regions we are selling and Managers of corresponding region"
            "\n6. Print different Shipment Modes and their Percentage Usability of all Shipments"
            "\n7. Create a New column 'Shipped Duration' which stores difference in days between Shipment and Order Date"
            "\n8. Order Ids with Shipped Duration > 10 Days"
            "\n9. List of Orders returned where Shipped Duration > 15 Days and its corresponding Manager "
            "\n10. Group By Region and which region is more profitable"
            "\n11. Which Country gives more discount"
            "\n12. List fo Unique PostCode"
            "\n13. Which customer segment is more profitable"
            "\n14. 10th most loss making product"
            "\n15. Top 10 product with highest margin"
            "\nEnter Your Choice :"))
        if choice2 == 1:
            db_print_dataset()
        elif choice2 == 2:
            returns_show()
        elif choice2 == 3:
            join_order_return()
        elif choice2 == 4:
            unique_customer_list()
        elif choice2 == 5:
            region_manager_list()
        elif choice2 == 6:
            shipment_list()
        elif choice2 == 7:
            db_not_active_regularly_sql()
        elif choice2 == 8:
            db_third_most_activey_sql()
            db_third_most_activey_sql_alternate()
        elif choice2 == 9:
            db_fifth_most_laziest_sql()
        elif choice2 == 10:
            db_cumulative_calories_burn_sql()

        else:
            print("Return to Main Operations")
            sql_oper_flag = False

def db_connect():
    mydb1 = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",use_pure=True)
    return(mydb1)

def db_print_dataset():
    try:
        mydb1 = db_connect()
        cursor1 = mydb1.cursor()

        print("Show SuperStore Orders records")
        orders_query = "select * from superstore_usa_orders; "
        cursor1.execute(orders_query)
        print(f"SuperStore_Orders data reading using cursor\n {cursor1.fetchall()}")

        print("Show SuperStore Returns records")
        returns_query = "select * from superstore_usa_returns; "
        cursor1.execute(returns_query)
        print(f"SuperStore_Orders data reading using cursor\n {cursor1.fetchall()}")

        print("Show SuperStore Regions records")
        returns_query = "select * from superstore_usa_regions; "
        cursor1.execute(returns_query)
        print(f"SuperStore_Regions data reading using cursor\n {cursor1.fetchall()}")


        # print(f"\nSuperStore_Orders data reading using read_sql\n")
        # print(pd.read_sql(orders_query, mydb1))
    except Exception as e:
            mydb1.close()
            print(f"Error in Reading Data: {e}")

# "\n2. How Many returns received and what are their ID's"
def returns_show():
    try:
        mydb1 = db_connect()
        cursor1 = mydb1.cursor()
        print("Show Returns Orders records")
        returns_count_query = "select count(*) from superstore_usa_returns; "
        cursor1.execute(returns_count_query)
        print(f"Total no of Returns \n {cursor1.fetchall()}")

        returns_query = "select OrderID from superstore_usa_returns; "
        cursor1.execute(returns_query)
        print(f"Order ID which were returned \n {cursor1.fetchall()}")

    except Exception as e:
            mydb1.close()
            print(f"Error in Reading Data: {e}")


# "\n3. Join Order and Return File and print it"
def join_order_return():
    try:
        mydb1 = db_connect()
        cursor1 = mydb1.cursor()
        print("Join of Orders and Return records")
        returns_count_query = "select * from superstore_usa_orders so join superstore_usa_returns sr on so.Order_ID = sr.Order_ID; "
        cursor1.execute(returns_count_query)
        print(f"Join of ORders and Returns \n {cursor1.fetchall()}")
    except Exception as e:
            mydb1.close()
            print(f"Error in Join Data: {e}")

# "\n4. Print Unique Customer Lists"
def unique_customer_list():
    try:
        mydb1 = db_connect()
        cursor1 = mydb1.cursor()
        unique_customer_query = "select count(distinct(Customer_Name)) from superstore_usa_orders ; "
        cursor1.execute(unique_customer_query)
        print(f"Total count of Unique Customer  :  {cursor1.fetchall()}")

        unique_customer_list_query = "select distinct(Customer_Name) from superstore_usa_orders ; "
        cursor1.execute(unique_customer_list_query)
        print(f"Unique Customer List :  {cursor1.fetchall()}")
    except Exception as e:
            mydb1.close()
            print(f"Error in Join Data: {e}")


# "\n5. How Many regions we are selling and Managers of corresponding region"
def region_manager_list():
    try:
        mydb1 = db_connect()
        cursor1 = mydb1.cursor()
        returns_count_query = "select so.Region, sr.Manager from superstore_usa_orders so join superstore_usa_returns sr on so.Region = sr.Region; "
        cursor1.execute(returns_count_query)
        print(f"Join of Orders and Region records \n {cursor1.fetchall()}")
    except Exception as e:
            mydb1.close()
            print(f"Error in Orders and Returns: {e}")

# "\n6. Print different Shipment Modes and their Percentage Usability of all Shipments"
def shipment_list():
    try:
        mydb1 = db_connect()
        cursor1 = mydb1.cursor()
        shipment_count_query = "SELECT Ship_Mode, count(Ship_Mode) as count_ship_mode,round(((count(Order_Id) * 100) / temp.total_count),2) AS Percentage_count" \
                               " FROM superstore_usa_orders JOIN (SELECT count(Order_Id) AS total_count FROM superstore_usa_orders) temp group by Ship_Mode; "
        cursor1.execute(shipment_count_query)
        print(f"Shipment Modes and Percentage Usability \n {cursor1.fetchall()}")
    except Exception as e:
            mydb1.close()
            print(f"Error in Shipment Modes and Percentage Usability: {e}")



# "\n7. Create a New column 'Shipped Duration' which stores difference in days between Shipment and Order Date"
def shipped_order_date():
    try:
        mydb1 = db_connect()
        cursor1 = mydb1.cursor()
        print("Show Database records with converted Date and Time")
        read_date_query = "select Order_Date, STR_TO_DATE(Order_Date, '%m/%d/%YY') as new_Order_Date,  Ship_Date, STR_TO_DATE(Ship_Date, '%m/%d/%YY') as new_Ship_Date from superstore_usa_orders; "
        cursor1.execute(read_date_query)
        print(f"SuperStore_Orders data reading using cursor\n {cursor1.fetchall()}")
        print(f"\nSuperStore_Orders data reading using read_sql\n")
        print(pd.read_sql(read_date_query, mydb1))
    except Exception as e:
            mydb1.close()
            print(f"Error in Converting Date Time format : {e}")