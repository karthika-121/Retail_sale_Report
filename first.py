import os
import urllib.request
import ssl
from itertools import groupby

from pyspark.sql.functions import count
from unicodedata import category

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r''        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

import os

os.environ["HADOOP_HOME"] = "C://hadoop"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("RetailETL").getOrCreate()

# Load both Raw sheets
book1_df = spark.read.csv("file:///D:/project/online_retail_II_sheet1.csv", header=True, inferSchema=True)
#book1_df.show()
#sheet1.printSchema()
#print("Total rows of Year 2009-2010:",book1_df.count())
# TRANSFORMATION
book1_droped_duplicates= book1_df.dropDuplicates()

book1_timestamped = book1_droped_duplicates.withColumn(
    "InvoiceDate",
    F.to_timestamp("InvoiceDate", "dd-MM-yy HH:mm")
)
book1_dropNull =book1_timestamped.na.drop(subset=["Invoice", "StockCode", "Description"])
book1_removedCancellation = book1_dropNull.filter(~book1_dropNull.Invoice.startswith("C"))
book1_saleRevenue = book1_removedCancellation.withColumn("TotalAmount", col("Quantity") * col("Price"))

book1_isReturn = book1_saleRevenue.withColumn(
    "Is_Return",
    F.when(F.col("Quantity") < 0, True).otherwise(False)
)
print("=============Cleaned data after transformation=================")
book1_cleaned = (
   book1_isReturn
    .withColumn("Year", F.year("InvoiceDate"))
    .withColumn("Month", F.month("InvoiceDate"))
    .withColumn("Day", F.to_date("InvoiceDate"))
    .withColumn("Hour", F.hour("InvoiceDate"))
                )
book1_cleaned.show()
print(book1_cleaned.count())
book1_cleaned = book1_cleaned.withColumnRenamed("Customer ID", "CustomerID")

#   Separate cancelled invoices into a returns/cancellations table
book1_isReturn.groupBy("Is_Return").count().show()
#  Set the legacy time policy to handle non-standard date formats
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
returns_df= book1_isReturn.filter(F.col("Is_Return"))
book1_saleRevenue.printSchema()
returns_df.show()
print(returns_df.count())
#   Remove rows with price less than or equal to zero from the sales table
sales_df = book1_cleaned.filter(
    (F.col("Quantity") > 0) &
    (F.col("Price") > 0) &
    (F.col("InvoiceDate").isNotNull())
)
sales_df.show(5)
print(sales_df.count())
'''sales_df.printSchema()
# 1. Rename the column to remove the space /
2. Now write to Parquet successfully    /


book1_cleaned.write.mode("overwrite").format("csv").save("file:///D:/project/project1_sales/processed/cleaned")
#book1_cleaned.write.mode("overwrite").parquet("file:///D:/project/project1_sales/processed/cleaned")
#sales_df.write.mode("overwrite").format("parquet").save("file:///D:/project/project1_sales/processed/sales")
#returns_df.write.mode("overwrite").parquet("file:///D:/project/project1_sales/processed/returns")

'''

#                                                                 Product Dimension
dim_product = sales_df.select(
    "StockCode", "Description"
).dropDuplicates(["StockCode"])
dim_product.show()
print("dim_product is Product Dimension: ",dim_product.count())
#                                                                    customer Dimension
dim_customer = sales_df.select(
    "CustomerID", "Country"
).dropDuplicates(["CustomerID"])

print("dim_customer is customer Dimension: ",dim_customer.count())
dim_customer.show()

#                                                                   cleaning Completed here###############################
################################################################################################################
# ==========================================
# PART 1: REVENUE ANALYSIS
# ==========================================
print("===========1.    Total_Revenue==========")
Total_Revenue= (sales_df
                .agg(sum("TotalAmount")
                     .alias("Total_Revenue")
                     )
                )

Total_Revenue.show()

#   Monthly_Revenue
Monthly_Revenue= (sales_df
                  .groupby("Year","Month")
                  .agg(F.sum("TotalAmount")
                       .alias("Monthly_Revenue"))
                  .orderBy("Year","Month"))
print("===========2. Monthly_Revenue==========")
Monthly_Revenue.show()

#   Yearly_Revenue
Yearly_Revenue=(sales_df
                .groupby("Year")
                .agg(F.sum("TotalAmount")
                     .alias("Yearly_Revenue"))
                .orderBy("Year"))
print("===========3. Yearly_Revenue==========")
Yearly_Revenue.show()
print("===========4. dow_Revenue==========")
#   Revenue by day of week
dow_Revenue=(sales_df
             .groupBy("Day")
             .agg(F.sum("TotalAmount")
                  .alias("Daily_Revenue"))
             .orderBy(F.sum("TotalAmount")
                      .desc()))

dow_Revenue.show()
print("===========5.  Revenue by hour of day==========")
#
Hourly_Revenue=(sales_df
                .groupBy("Hour")
                .agg(F.sum("TotalAmount")
                     .alias("Hourly_Revenue"))
                .orderBy(F.sum("TotalAmount")
                         .desc()))

Hourly_Revenue.show()
print("===========6. Revenue by country==========")
Country_Revenue=(sales_df
                 .groupBy("Country")
                 .agg(F.sum("TotalAmount")
                      .alias("Country_Revenue"))
                 .orderBy(F.sum("TotalAmount")
                          .desc()))
Country_Revenue.show()

print("===========7. Average_order_value==========")
#   Average order value(AOV)--> Formula: Total Revenue / Total Unique Orders
Average_order_value=(sales_df
                     .agg((F.sum("TotalAmount")/F.count_distinct("StockCode"))
                          .alias("Average_order_value")))

Average_order_value.show()


######################################## Product performance##############
# ==========================================
# PART 2: PRODUCT & INVENTORY ANALYSIS
# ==========================================
print("===========8. Top 10 products by revenue==========")
TopTen= (sales_df.groupby("StockCode","Description")
         .agg(F.sum("TotalAmount").alias("TopTen"))
         .orderBy(F.col("TopTen").desc()).limit(10))

TopTen.show()
TopTen.show(truncate=False)

print("===========9. Top 10 products by quantity sold==========")
TopTen_by_quantity= (sales_df.groupby("StockCode","Description")
                     .agg(F.sum("Quantity").alias("TopTen_by_quantity"))
                     .orderBy(F.col("TopTen_by_quantity").desc()).limit(10))

TopTen_by_quantity.show()
TopTen_by_quantity.show(truncate=False)

print("===========10. Most_returned_products==========")
Most_returned_product= (returns_df.groupby("StockCode","Description")
                                    .agg(F.sum(abs("Quantity")).alias("Most_returned_products"))
                                    .orderBy(F.col("Most_returned_products").desc()).limit(10))
Most_returned_product.show()

print("===========11. # Total units sold per product from sales_df==========")
units_sold_per_product=(sales_df.groupBy("StockCode","Description")
                .agg(F.sum("Quantity")
                .alias("units_sold per_product"))
                .orderBy(F.col("units_sold per_product").desc()).limit(10))

units_sold_per_product.show()

print("=== 12. Products with High Revenue but Low Quantity ===")
#1. Filter for luxury / high-value items (e.g., Revenue > $5,000 and Units Sold < 100)
#2. Aggregate sales revenue and quantity per product

product_performance=(sales_df
                     .filter(
                            (~col("Is_Return")&
                             (F.col("Quantity")>0))
                            )
                     .groupBy("StockCode","Description")
                     .agg(F.sum("TotalAmount").alias("Total_revenue"),F.sum("Quantity").alias("Total_quantity"))
                     .withColumn("avg_unit_price",col("Total_revenue") / col("Total_quantity"))
)

high_rev_low_qty= (product_performance
                   .filter(
                        (col("Total_revenue")>5000 )&
                        (col("Total_quantity")<100)
                            )
                   .orderBy(desc("avg_unit_price"))
                   )
high_rev_low_qty.show(truncate=False)

print("=== 13. Products with Declining Monthly Sales ===")
# 1. Calculate monthly revenue per product using pre-existing Year and Month in sales_df
# 2. Set up a window partitioned by product and ordered chronologically
# 3. Compare current month revenue to previous month using lag()
# 4. Filter for severe drops (e.g., > 30% month-over-month decline)

monthly_sales=(sales_df
                    .filter(~col("Is_Return"))
                    .groupBy("StockCode","Description","Year","Month")
                    .agg(
                       F.sum("TotalAmount").alias("monthly_sales")
                        )
               )
month_window = Window.partitionBy("StockCode").orderBy("Year", "Month")
# Percentage Change= (currentRevenue-Previous Revenue)/Previous Revenue *100

monthly_trend=(monthly_sales
               .withColumn("previous_month",lag("monthly_sales",1).over(month_window))
               .withColumn("percentage_change",
                           ((col("monthly_sales")-col("previous_month"))/col("previous_month")) *100)
               )
declining_products=(monthly_trend
                    .filter(
                            (col("percentage_change") < -30) &
                            (col("previous_month").isNotNull() )
                          )
                    .orderBy("Year","Month","percentage_change")
                    )
declining_products.show(10,truncate=False)

#   =============================================================================================
#       Part3.  Customer analysis
#   =============================================================================================
print("==========14. Total active customers=============")

total_customer = (
    sales_df
    .select(
        count_distinct("CustomerID", "Country").alias("Total_active_customer")
    )
    .orderBy("Total_active_customer")
)

total_customer.show()








































