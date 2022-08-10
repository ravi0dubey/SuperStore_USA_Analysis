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
            "\nPandas Operataions\n1 Load and Print dataset \n2. How Many returns received and what are their ID's"
            "\n3. Join Order and Return File and print it\n4.Print Unique Customer Lists\n5.How Many regions we are selling and Managers of corresponding region"
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
            "\n. Enter Your Choice :"))
        if choice2 == 1:
            db_date_time()
        elif choice2 == 2:
            db_print_unique_id_sql()
        elif choice2 == 3:
            db_active_id_sql()
        elif choice2 == 4:
            db_activity_not_logged_sql()
        elif choice2 == 5:
            db_laziest_sql()
        elif choice2 == 6:
            db_fit_accordingto_calories_burn_sql()
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