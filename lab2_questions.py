"""
LAB 2 - Part C: Business Questions (Standalone Script)
Loads CSVs and answers the 5 questions using Spark DataFrames only.
"""
import os
import sys

# Improve encoding on Windows terminals
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Ensure Spark uses current Python interpreter (robust to spaces/missing path)
py_exec = sys.executable if os.path.exists(sys.executable) else "python"
os.environ['PYSPARK_PYTHON'] = py_exec
os.environ['PYSPARK_DRIVER_PYTHON'] = py_exec

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, month, to_date, desc, round as spark_round

print("=" * 80)
print("LAB 2 - PART C: BUSINESS QUESTIONS")
print("=" * 80)

spark = (
    SparkSession.builder
    .appName("Day1-Questions")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load datasets
customers = spark.read.option("header", "true").option("inferSchema", "true").csv("spark-data/ecommerce/customers.csv").cache()
products = spark.read.option("header", "true").option("inferSchema", "true").csv("spark-data/ecommerce/products.csv").cache()
orders = spark.read.option("header", "true").option("inferSchema", "true").csv("spark-data/ecommerce/orders.csv").cache()

print("\n✓ Datasets loaded (customers, products, orders)")

# Q1: Country with highest total credit limit
print("\n--- Q1: Country with highest total credit limit ---")
q1 = customers.groupBy("country").agg(spark_round(spark_sum("creditLimit"), 2).alias("total_creditLimit")).orderBy(desc("total_creditLimit")).limit(1)
q1.show(truncate=False)

# Q2: Most common order status
print("\n--- Q2: Most common order status ---")
q2 = orders.groupBy("status").count().orderBy(desc("count")).limit(1)
q2.show(truncate=False)

# Q3: Product category with most stock
print("\n--- Q3: Product category with most stock ---")
q3 = products.groupBy("productCategory").agg(spark_sum("quantityInStock").alias("total_stock")).orderBy(desc("total_stock")).limit(1)
q3.show(truncate=False)

# Q4: Percentage of Enterprise customers
print("\n--- Q4: Percentage of Enterprise customers ---")
all_customers = customers.count()
enterprise_customers = customers.filter(col("customerSegment") == "Enterprise").count()
percentage = (enterprise_customers / all_customers) * 100 if all_customers else 0.0
print(f"Total customers: {all_customers}")
print(f"Enterprise customers: {enterprise_customers}")
print(f"Percentage Enterprise: {percentage:.2f}%")

# Q5: Distribution of orders by month
print("\n--- Q5: Distribution of orders by month ---")
orders_m = orders.withColumn("orderDate_parsed", to_date(col("orderDate"), "yyyy-MM-dd")).withColumn("order_month", month(col("orderDate_parsed")))
q5 = orders_m.groupBy("order_month").count().orderBy("order_month")
q5.show(12)

print("\n" + "=" * 80)
print("Questions completed. Stopping Spark...")
spark.stop()
print("✓ Spark stopped")
