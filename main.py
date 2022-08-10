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

import SQL_Connection as sc
import SQL_Operations as so
import Pandas_Operations as po

fitbit_flag_operations = True
while fitbit_flag_operations:
    choice = int(input("\n1.My SQL Data Load \n2.SQL Data_Operations\n3.Pandas\n\nEnter Your Choice: "))
    if choice == 1:
        sc.mysql_db_connection()
    elif choice == 2:
        so.sql_db_operations()
    elif choice == 3:
        po.pandas_db_operations()
    else:
        print("have a good day")
        fitbit_flag_operations = False
