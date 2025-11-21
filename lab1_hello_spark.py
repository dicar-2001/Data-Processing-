"""
LAB 1.1 - First Spark Application
Student: [VOTRE NOM]
Date: 2024

This application demonstrates:
- SparkSession initialization
- DataFrame creation from static data
- Schema inspection
- Transformations (filter, select, groupBy, orderBy)
- Actions (show, count, collect)
- Aggregations (avg, count, sum)
"""
import os
import sys

# Fix encoding for Windows PowerShell
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# CRITICAL: Set Python executable for Spark workers
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum, min as spark_min, max as spark_max

def main():
    # ==================== STEP 1: Initialize SparkSession ====================
    print("=" * 80)
    print("LAB 1.1 - FIRST SPARK APPLICATION")
    print("=" * 80)
    
    print("\n[STEP 1] Initializing SparkSession...")
    print("-" * 80)
    
    spark = SparkSession.builder \
        .appName("Lab1_HelloSpark") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    
    print("✓ SparkSession created successfully")
    print(f"  Application Name: {spark.sparkContext.appName}")
    print(f"  Spark Version: {spark.version}")
    print(f"  Master URL: {spark.sparkContext.master}")
    print(f"  Application UI: http://localhost:4040")
    
    # ==================== STEP 2: Display Environment Info ====================
    print("\n[STEP 2] Environment Information")
    print("-" * 80)
    
    sc = spark.sparkContext
    print(f"  Application ID: {sc.applicationId}")
    print(f"  Default Parallelism: {sc.defaultParallelism}")
    print(f"  Spark User: {sc.sparkUser()}")
    print(f"  Python Version: {sc.pythonVer}")
    
    # ==================== STEP 3: Create DataFrame from Static Data ====================
    print("\n[STEP 3] Creating DataFrame from Static Data")
    print("-" * 80)
    
    # Sample data: E-commerce orders
    data = [
        (1, "Alice", "Electronics", 1200.50, "2024-01-15", "Completed"),
        (2, "Bob", "Clothing", 85.00, "2024-01-16", "Completed"),
        (3, "Charlie", "Electronics", 450.75, "2024-01-17", "Pending"),
        (4, "Diana", "Books", 35.00, "2024-01-18", "Completed"),
        (5, "Eve", "Electronics", 899.99, "2024-01-19", "Completed"),
        (6, "Frank", "Clothing", 120.00, "2024-01-20", "Cancelled"),
        (7, "Grace", "Books", 45.50, "2024-01-21", "Completed"),
        (8, "Henry", "Electronics", 1500.00, "2024-01-22", "Completed"),
        (9, "Ivy", "Clothing", 200.00, "2024-01-23", "Completed"),
        (10, "Jack", "Books", 28.75, "2024-01-24", "Pending"),
        (11, "Kate", "Electronics", 675.00, "2024-01-25", "Completed"),
        (12, "Liam", "Clothing", 95.50, "2024-01-26", "Completed")
    ]
    
    columns = ["order_id", "customer", "category", "amount", "date", "status"]
    
    df = spark.createDataFrame(data, schema=columns)
    
    print(f"✓ DataFrame created with {len(data)} records")
    print(f"  Columns: {', '.join(columns)}")
    
    # ==================== STEP 4: Show Schema ====================
    print("\n[STEP 4] DataFrame Schema")
    print("-" * 80)
    df.printSchema()
    
    # ==================== STEP 5: Preview Data ====================
    print("\n[STEP 5] Preview Data (First 5 rows)")
    print("-" * 80)
    df.show(5, truncate=False)
    
    # ==================== STEP 6: Count Total Records ====================
    print("\n[STEP 6] Count Total Records")
    print("-" * 80)
    total_count = df.count()
    print(f"Total number of records: {total_count}")
    
    # ==================== STEP 7: Aggregation by Category ====================
    print("\n[STEP 7] Aggregation - Statistics by Category")
    print("-" * 80)
    
    category_stats = df.groupBy("category") \
        .agg(
            count("*").alias("total_orders"),
            spark_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            spark_min("amount").alias("min_amount"),
            spark_max("amount").alias("max_amount")
        ) \
        .orderBy(col("total_revenue").desc())
    
    category_stats.show(truncate=False)
    
    # ==================== STEP 8: Filter - Completed Orders ====================
    print("\n[STEP 8] Filter - Completed Orders Only")
    print("-" * 80)
    
    completed_orders = df.filter(col("status") == "Completed")
    completed_count = completed_orders.count()
    
    print(f"Number of completed orders: {completed_count}")
    completed_orders.show(truncate=False)
    
    # ==================== STEP 9: Filter - High Value Orders ====================
    print("\n[STEP 9] Filter - High Value Orders (Amount >= 500)")
    print("-" * 80)
    
    high_value_orders = df.filter(col("amount") >= 500.0)
    
    print(f"Number of high value orders: {high_value_orders.count()}")
    high_value_orders.select("customer", "category", "amount", "status").show()
    
    # ==================== STEP 10: Complex Query ====================
    print("\n[STEP 10] Complex Query - Completed Electronics Orders > 500")
    print("-" * 80)
    
    electronics_high_value = df.filter(
        (col("category") == "Electronics") &
        (col("status") == "Completed") &
        (col("amount") > 500)
    ).select("customer", "amount", "date") \
     .orderBy(col("amount").desc())
    
    electronics_high_value.show()
    
    # ==================== STEP 11: Aggregation by Status ====================
    print("\n[STEP 11] Order Distribution by Status")
    print("-" * 80)
    
    status_distribution = df.groupBy("status") \
        .agg(
            count("*").alias("order_count"),
            spark_sum("amount").alias("total_amount")
        ) \
        .orderBy(col("order_count").desc())
    
    status_distribution.show()
    
    # ==================== STEP 12: Summary Statistics ====================
    print("\n[STEP 12] Summary Statistics for Order Amounts")
    print("-" * 80)
    df.select("amount").describe().show()
    
    # ==================== STEP 13: Top Customers ====================
    print("\n[STEP 13] Top 5 Customers by Total Spending")
    print("-" * 80)
    
    top_customers = df.groupBy("customer") \
        .agg(spark_sum("amount").alias("total_spent")) \
        .orderBy(col("total_spent").desc()) \
        .limit(5)
    
    top_customers.show()
    
    # ==================== Application Completed ====================
    print("\n" + "=" * 80)
    print("APPLICATION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\n📊 Spark Application UI is available at: http://localhost:4040")
    print("\n Navigate to:")
    print("  • Jobs Tab → Overview of all executed jobs")
    print("  • Stages Tab → Breakdown of jobs into stages")
    print("  • DAG Tab → Visual representation of execution plan")
    print("  • Executors Tab → Executor resources and metrics")
    print("\n⚠ IMPORTANT: Keep this application running to access the UI!")
    print("  The UI at port 4040 will close when the application stops.")
    print("\n Press Enter when you have finished taking screenshots...")
    
    # Keep the application running for UI exploration
    try:
        input()
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
    
    # ==================== STEP 14: Stop SparkSession ====================
    print("\n[STEP 14] Stopping SparkSession...")
    spark.stop()
    print("✓ SparkSession stopped successfully")
    
    print("=" * 80)

if __name__ == "__main__":
    main()