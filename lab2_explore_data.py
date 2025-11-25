"""
LAB 2 - E-Commerce Dataset Exploration (DataFrames)
Assignment: Load and explore e-commerce data using Spark DataFrames

Part B: Load & Explore Data with Spark
Part C: Business Questions with Spark

Team: Rachid Ait Ali, Oussama Madioubi, Said Mouhadabi, Chdia El Kharmoudi
Date: November 24, 2025
"""

import os
import sys

# Fix encoding for Windows PowerShell
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# CRITICAL: Set Python executable for Spark workers (robust to spaces/missing path)
py_exec = sys.executable if os.path.exists(sys.executable) else "python"
os.environ['PYSPARK_PYTHON'] = py_exec
os.environ['PYSPARK_DRIVER_PYTHON'] = py_exec

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, 
    min as spark_min, max as spark_max, round as spark_round, 
    lit, desc, month, to_date
)

def print_header(title):
    """Print formatted section header"""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

def print_section(section_number, title):
    """Print formatted section header"""
    print(f"\n{'=' * 80}")
    print(f"SECTION {section_number}: {title}")
    print("=" * 80)

# ============================================================================
# SECTION 2.1: Create SparkSession
# ============================================================================

print_header("LAB 2 - E-COMMERCE DATASET EXPLORATION")
print_section(2.1, "Create SparkSession")

spark = (
    SparkSession.builder
    .appName("Day1-DataExploration")
    .master("local[*]")  # Using local mode instead of spark://localhost:7077
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("✓ SparkSession created successfully")
print(f"  Application Name: Day1-DataExploration")
print(f"  Master: local[*]")
print(f"  Driver Memory: 2g")
print(f"  Spark Version: {spark.version}")

# ============================================================================
# SECTION 2.2: Load the three CSV datasets
# ============================================================================

print_section(2.2, "Load CSV Datasets")

print("\nLoading customers.csv...")
customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("spark-data/ecommerce/customers.csv") \
    .cache()

print("✓ Successfully loaded customers.csv")

print("\nLoading products.csv...")
products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("spark-data/ecommerce/products.csv") \
    .cache()

print("✓ Successfully loaded products.csv")

print("\nLoading orders.csv...")
orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("spark-data/ecommerce/orders.csv") \
    .cache()

print("✓ Successfully loaded orders.csv")

print("\n✓ All CSV datasets loaded and cached successfully!")

# ============================================================================
# SECTION 2.3: Inspect schemas
# ============================================================================

print_section(2.3, "Inspect Schemas")

print("\nCUSTOMERS Schema:")
customers.printSchema()

print("\nPRODUCTS Schema:")
products.printSchema()

print("\nORDERS Schema:")
orders.printSchema()

# ============================================================================
# SECTION 2.4: Basic statistics (size)
# ============================================================================

print_section(2.4, "Basic Statistics")

customer_count = customers.count()
product_count = products.count()
order_count = orders.count()

print(f"\n📊 Dataset Sizes:")
print(f"  • Total number of customers: {customer_count}")
print(f"  • Total number of products: {product_count}")
print(f"  • Total number of orders: {order_count}")

# ============================================================================
# SECTION 2.5: Data preview
# ============================================================================

print_section(2.5, "Data Preview")

print("\nCUSTOMERS (first 5 rows):")
customers.show(5, truncate=False)

print("\nPRODUCTS (first 5 rows):")
products.show(5, truncate=False)

print("\nORDERS (first 5 rows):")
orders.show(5, truncate=False)

# ============================================================================
# SECTION 2.6: Data quality checks
# ============================================================================

print_section(2.6, "Data Quality Checks")

print("\n--- NULL Values Analysis ---")

print("\nNULL values in CUSTOMERS:")
customers_nulls = customers.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
    for c in customers.columns
])
customers_nulls.show()

print("\nNULL values in ORDERS:")
orders_nulls = orders.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
    for c in orders.columns
])
orders_nulls.show()

print("\n--- Duplicate ID Checks ---")

print("\nChecking duplicate customerNumber:")
total_customers = customers.count()
distinct_customers = customers.select("customerNumber").distinct().count()
customer_duplicates = total_customers - distinct_customers
print(f"  Total customers: {total_customers}")
print(f"  Distinct customerNumbers: {distinct_customers}")
print(f"  Duplicates: {customer_duplicates}")

print("\nChecking duplicate orderNumber:")
total_orders = orders.count()
distinct_orders = orders.select("orderNumber").distinct().count()
order_duplicates = total_orders - distinct_orders
print(f"  Total orders: {total_orders}")
print(f"  Distinct orderNumbers: {distinct_orders}")
print(f"  Duplicates: {order_duplicates}")

# ============================================================================
# SECTION 2.7: Exploratory analysis
# ============================================================================

print_section(2.7, "Exploratory Analysis")

print("\n2.7.1 - Customers by segment:")
customers.groupBy("customerSegment") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("\n2.7.2 - Top 10 countries by customer count:")
customers.groupBy("country") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

print("\n2.7.3 - Orders by status:")
orders.groupBy("status") \
    .count() \
    .show()

print("\n2.7.4 - Orders by payment method:")
orders.groupBy("paymentMethod") \
    .count() \
    .show()

print("\n2.7.5 - Products by category:")
products.groupBy("productCategory") \
    .count() \
    .show()

# ============================================================================
# SECTION 2.8: Numerical analysis
# ============================================================================

print_section(2.8, "Numerical Analysis")

print("\n2.8.1 - Order amount statistics:")
order_stats = orders.select(
    count("totalAmount").alias("count"),
    spark_min("totalAmount").alias("min"),
    spark_max("totalAmount").alias("max"),
    spark_round(avg("totalAmount"), 2).alias("average"),
    spark_round(spark_sum("totalAmount"), 2).alias("total_sum")
)
order_stats.show()

print("\n2.8.2 - Credit limit statistics by segment:")
credit_stats = customers.groupBy("customerSegment").agg(
    count("*").alias("count"),
    spark_round(avg("creditLimit"), 2).alias("average_credit"),
    spark_round(spark_max("creditLimit"), 2).alias("max_credit")
)
credit_stats.show()

print("\n2.8.3 - Product price statistics:")
price_stats = products.select(
    spark_round(spark_min("buyPrice"), 2).alias("min_buyPrice"),
    spark_round(spark_max("buyPrice"), 2).alias("max_buyPrice"),
    spark_round(avg("buyPrice"), 2).alias("avg_buyPrice"),
    spark_round(spark_min("MSRP"), 2).alias("min_MSRP"),
    spark_round(spark_max("MSRP"), 2).alias("max_MSRP"),
    spark_round(avg("MSRP"), 2).alias("avg_MSRP")
)
price_stats.show()

# ============================================================================
# SECTION 2.9: Summary report export
# ============================================================================

print_section(2.9, "Summary Report Export")

# Compute summary metrics
total_customers = customer_count
total_products = product_count
total_orders = order_count
total_revenue = orders.agg(spark_sum("totalAmount")).first()[0]
avg_order_value = orders.agg(avg("totalAmount")).first()[0]

print(f"\nSummary Metrics:")
print(f"  • Total Customers: {total_customers}")
print(f"  • Total Products: {total_products}")
print(f"  • Total Orders: {total_orders}")
print(f"  • Total Revenue: ${total_revenue:,.2f}")
print(f"  • Average Order Value: ${avg_order_value:.2f}")

# Create summary DataFrame (convert all values to float for consistent type)
summary_data = [
    ("Total Customers", float(total_customers)),
    ("Total Products", float(total_products)),
    ("Total Orders", float(total_orders)),
    ("Total Revenue", round(float(total_revenue), 2)),
    ("Average Order Value", round(float(avg_order_value), 2))
]

summary = spark.createDataFrame(summary_data, ["Metric", "Value"])

print("\nSummary DataFrame:")
summary.show(truncate=False)

# Write summary to CSV using Python (workaround for Windows HADOOP_HOME issue)
print("\nWriting summary to CSV...")
import csv
import os

# Create summary directory if it doesn't exist
os.makedirs("spark-data/ecommerce/summary", exist_ok=True)

# Collect data and write using Python's csv module
summary_rows = summary.collect()
with open("spark-data/ecommerce/summary/summary_report.csv", "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["Metric", "Value"])
    for row in summary_rows:
        writer.writerow([row["Metric"], row["Value"]])

print("✓ Summary exported to: spark-data/ecommerce/summary/summary_report.csv")

# ============================================================================
# SECTION 9: BUSINESS QUESTIONS (Part C)
# ============================================================================

print_section(9, "Business Questions (Part C)")

# Question 1: Country with highest total credit limit
print("\n--- QUESTION 1: Country with highest total credit limit ---")
q1_result = customers.groupBy("country") \
    .agg(spark_round(spark_sum("creditLimit"), 2).alias("total_creditLimit")) \
    .orderBy(col("total_creditLimit").desc()) \
    .limit(1)

q1_result.show(truncate=False)
q1_data = q1_result.first()
print(f"Answer: {q1_data['country']} with total credit limit of ${q1_data['total_creditLimit']:,.2f}")

# Question 2: Most common order status
print("\n--- QUESTION 2: Most common order status ---")
q2_result = orders.groupBy("status") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(1)

q2_result.show()
q2_data = q2_result.first()
print(f"Answer: {q2_data['status']} with {q2_data['count']} orders")

# Question 3: Product category with most stock
print("\n--- QUESTION 3: Product category with most stock ---")
q3_result = products.groupBy("productCategory") \
    .agg(spark_sum("quantityInStock").alias("total_stock")) \
    .orderBy(col("total_stock").desc()) \
    .limit(1)

q3_result.show(truncate=False)
q3_data = q3_result.first()
print(f"Answer: {q3_data['productCategory']} with {q3_data['total_stock']} units in stock")

# Question 4: Percentage of Enterprise customers
print("\n--- QUESTION 4: Percentage of Enterprise customers ---")
total_customers = customers.count()
enterprise_customers = customers.filter(col("customerSegment") == "Enterprise").count()
percentage = (enterprise_customers / total_customers) * 100

print(f"Total customers: {total_customers}")
print(f"Enterprise customers: {enterprise_customers}")
print(f"Answer: {percentage:.2f}% of customers are Enterprise segment")

# Question 5: Distribution of orders by month
print("\n--- QUESTION 5: Distribution of orders by month ---")
# Convert orderDate to date and extract month
orders_with_month = orders.withColumn("orderDate_parsed", to_date(col("orderDate"), "yyyy-MM-dd")) \
    .withColumn("order_month", month(col("orderDate_parsed")))

q5_result = orders_with_month.groupBy("order_month") \
    .count() \
    .orderBy("order_month")

print("\nOrders per month:")
q5_result.show(12)

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print_header("LAB 2 COMPLETED SUCCESSFULLY")

print("\n✓ All sections completed:")
print("  2.1 - SparkSession created")
print("  2.2 - CSV datasets loaded")
print("  2.3 - Schemas inspected")
print("  2.4 - Basic statistics calculated")
print("  2.5 - Data previewed")
print("  2.6 - Data quality checks performed")
print("  2.7 - Exploratory analysis completed")
print("  2.8 - Numerical analysis completed")
print("  2.9 - Summary report exported")
print("  9   - Business questions answered")

print("\n📊 Final Summary:")
print(f"  • Total Customers: {total_customers}")
print(f"  • Total Products: {total_products}")
print(f"  • Total Orders: {total_orders}")
print(f"  • Total Revenue: ${total_revenue:,.2f}")
print(f"  • Average Order Value: ${avg_order_value:.2f}")
print(f"  • Customer Duplicates: {customer_duplicates}")
print(f"  • Order Duplicates: {order_duplicates}")

print("\n" + "=" * 80)
print("Stopping SparkSession...")
spark.stop()
print("✓ SparkSession stopped successfully")
print("=" * 80)
