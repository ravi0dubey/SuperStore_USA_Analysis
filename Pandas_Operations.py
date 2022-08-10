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
# pip install openpyxl



def pandas_db_operations():
    pandas_oper_flag = True
    while pandas_oper_flag:
        choice2 = int(input(
            "\nPandas Operataions\n1 Load and Print dataset "
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
            load_dataset()
        elif  choice2 == 2:
            returns_recieved()
        elif  choice2 == 3:
            order_return_join()
        elif choice2 == 4:
            unique_customer_list()
        elif choice2 == 5:
            regions_manager()
        elif choice2 == 6:
            shipment_mode_usability()
        elif choice2== 7:
            shipment_duration()
        elif choice2== 8:
            shipment_duration_10days()
        elif choice2== 9:
            shipment_duration_15days()
        # elif choice2== 10:
        #     db_cumulative_calories_burn_sql()

        else:
            print("Return to Main Operations")
            pandas_oper_flag = False


def load_orders():
    df_Orders =pd.read_excel(r'D:\Study\Data Science\Python\ineuron\Data_Set\Superstore_USA.xlsx', engine='openpyxl', sheet_name = 'Orders')
    return df_Orders

def load_returns():
    df_Returns =pd.read_excel(r'D:\Study\Data Science\Python\ineuron\Data_Set\Superstore_USA.xlsx', engine='openpyxl', sheet_name = 'Returns')
    return(df_Returns)

def load_users():
    df_Users=pd.read_excel(r'D:\Study\Data Science\Python\ineuron\Data_Set\Superstore_USA.xlsx', engine='openpyxl', sheet_name = 'Users')
    return (df_Users)

def load_dataset():
    df_orders= load_orders()
    print(f"Orders Dataset \n{df_orders}")
    df_returns = load_returns()
    print(f"Returns Dataset\n {df_returns}")
    df_users= load_users()
    print(f"Users Dataset\n{df_users}")

def returns_recieved():
    df_returns = load_returns()
    print(f"\nCount of returns received : {df_returns.count()}")
    print(f"\nOrderID's for which returns received :\n {df_returns['Order ID']}")
    print(f"\nCount of returns received for each OrderID: {df_returns.groupby('Order ID').count()}")

def order_return_join():
    df_orders = load_orders()
    df_returns = load_returns()
    print("Show status of Order\n")
    df1= pd.merge(df_orders, df_returns, on='Order ID', how="left")
    print(df1)
    print("Show details of al Orders which were not returned\n")
    print(df1[df1['Status']!= 'Returned'])
    print("Show details of al Orders which were returned\n")
    print(df1[df1['Status']== 'Returned'])


def unique_customer_list():
    df_orders = load_orders()
    print(f"We have total {df_orders['Customer Name'].nunique()} unique Customers\n")
    print(f"Details of Unique Customers are \n{pd.DataFrame(df_orders['Customer Name'].unique())}")

# "\n5. How Many regions we are selling and Managers of corresponding region"

def regions_manager():
    df_orders= load_orders()
    df_users= load_users()
    print(f"We have total {df_orders['Region'].nunique()} unique Region\n")
    print(f"Details of Unique Region with Managers are \n{df_users}")

# "\n6. Print different Shipment Modes and their Percentage Usability of all Shipments"
def shipment_mode_usability():
    df_orders= load_orders()
    print(f" Count of Different Shipping modes :  {df_orders.groupby('Ship Mode')[['Order ID']].agg(['count'])}")

#Calculating Shipment Duration
def shipment_duration_calcuation():
    df_orders= load_orders()
    df_orders['Order_date_date']=  pd.to_datetime(df_orders['Order Date'], infer_datetime_format=True)
    df_orders['Ship_date_date'] =  pd.to_datetime(df_orders['Ship Date'], infer_datetime_format=True)
    df_orders['Shipping_Duration'] = df_orders['Ship_date_date'] - df_orders['Order_date_date']
    return df_orders

# "\n7. Create a New column 'Shipped Duration' which stores difference in days between Shipment and Order Date"
def shipment_duration():
    df_orders= shipment_duration_calcuation()
    df_orders_15 = df_orders[df_orders['Shipping_Duration'] > '15 days']
    pd.merge(df_orders_15, df_users, on='Region', how="inner")


# "\n8. Order Ids with Shipped Duration > 10 Days"
def shipment_duration_10days():
    df_orders= shipment_duration_calcuation()
    print(f"Printing list of Orders greater than 10 days :\n{df_orders[df_orders['Shipping_Duration']> '10 days']}")


# "\n9. List of Orders returned where Shipped Duration > 15 Days and its corresponding Manager "
def shipment_duration_15days():
    df_orders= shipment_duration_calcuation()
    df_users = load_users()
    df_orders_15 = df_orders[df_orders['Shipping_Duration'] > '15 days']
    print(f"Printing list of Orders greater than 15 days along with manager :\n{pd.merge(df_orders_15,df_users, on = 'Region', how= 'inner')}")