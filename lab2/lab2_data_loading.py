"""
LAB 2 - Data Loading and Basic Transformations
Team: Rachid Ait Ali, Oussama Madioubi, Said Mouhadabi, Chdia El Kharmoudi
Date: 2024

This application demonstrates:
- Loading data from various sources (CSV, JSON, Parquet)
- Data exploration and profiling
- Basic transformations (select, filter, withColumn)
- Handling missing data
- Data type conversions
- Writing data to different formats
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
from pyspark.sql.functions import col, when, lit, upper, lower, concat_ws, round as spark_round
from pyspark.sql.types import IntegerType, DoubleType, StringType

def main():
    print("=" * 80)
    print("LAB 2 - DATA LOADING AND BASIC TRANSFORMATIONS")
    print("=" * 80)
    
    # ==================== STEP 1: Initialize SparkSession ====================
    print("\n[STEP 1] Initializing SparkSession...")
    print("-" * 80)
    
    spark = SparkSession.builder \
        .appName("Lab2_DataLoading") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    print("✓ SparkSession created successfully")
    print(f"  Spark Version: {spark.version}")
    
    # ==================== STEP 2: Create Sample Data ====================
    print("\n[STEP 2] Creating Sample E-commerce Dataset")
    print("-" * 80)
    
    # Sample data: Product sales
    data = [
        (1, "Laptop", "Electronics", 1200.50, 5, "2024-01-15", "Available"),
        (2, "Mouse", "Electronics", 25.99, 150, "2024-01-16", "Available"),
        (3, "Keyboard", "Electronics", 75.00, 80, "2024-01-17", "Available"),
        (4, "Monitor", "Electronics", 350.00, 30, "2024-01-18", "Available"),
        (5, "Desk Chair", "Furniture", 299.99, 20, "2024-01-19", "Available"),
        (6, "Desk", "Furniture", 450.00, 15, "2024-01-20", "Available"),
        (7, "Notebook", "Stationery", 5.50, 500, "2024-01-21", "Available"),
        (8, "Pen Set", "Stationery", 12.99, 300, "2024-01-22", "Available"),
        (9, "USB Cable", "Electronics", None, 200, "2024-01-23", "Available"),
        (10, "Headphones", "Electronics", 89.99, 0, "2024-01-24", "Out of Stock"),
        (11, None, "Electronics", 45.00, 100, "2024-01-25", "Available"),
        (12, "Webcam", "Electronics", 120.00, 25, None, "Available")
    ]
    
    columns = ["product_id", "product_name", "category", "price", "stock_quantity", 
               "last_updated", "status"]
    
    df = spark.createDataFrame(data, schema=columns)
    
    print(f"✓ Dataset created with {df.count()} records")
    print(f"  Columns: {', '.join(df.columns)}")
    
    # ==================== STEP 3: Data Exploration ====================
    print("\n[STEP 3] Data Exploration")
    print("-" * 80)
    
    print("\n3.1 - Schema Information:")
    df.printSchema()
    
    print("\n3.2 - First 5 rows:")
    df.show(5, truncate=False)
    
    print("\n3.3 - Summary Statistics:")
    df.describe().show()
    
    print("\n3.4 - Check for NULL values:")
    from pyspark.sql.functions import sum as spark_sum
    null_counts = df.select([spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
                             for c in df.columns])
    null_counts.show()
    
    # ==================== STEP 4: Basic Transformations ====================
    print("\n[STEP 4] Basic Transformations")
    print("-" * 80)
    
    print("\n4.1 - Select specific columns:")
    df.select("product_name", "category", "price").show(5)
    
    print("\n4.2 - Filter records (Electronics only):")
    electronics = df.filter(col("category") == "Electronics")
    print(f"  Found {electronics.count()} electronics products")
    electronics.show(5)
    
    print("\n4.3 - Filter with multiple conditions (Price > 50 AND stock > 50):")
    expensive_instock = df.filter((col("price") > 50) & (col("stock_quantity") > 50))
    expensive_instock.show()
    
    # ==================== STEP 5: Adding and Modifying Columns ====================
    print("\n[STEP 5] Adding and Modifying Columns")
    print("-" * 80)
    
    print("\n5.1 - Add calculated column (Total Value = Price × Stock):")
    df_with_value = df.withColumn("total_value", 
                                   col("price") * col("stock_quantity"))
    df_with_value.select("product_name", "price", "stock_quantity", "total_value").show(5)
    
    print("\n5.2 - Add stock status column:")
    df_enriched = df_with_value.withColumn("stock_status",
        when(col("stock_quantity") == 0, "Out of Stock")
        .when(col("stock_quantity") < 50, "Low Stock")
        .otherwise("In Stock")
    )
    df_enriched.select("product_name", "stock_quantity", "stock_status").show()
    
    print("\n5.3 - Add price category:")
    df_enriched = df_enriched.withColumn("price_category",
        when(col("price") >= 300, "Premium")
        .when(col("price") >= 100, "Mid-Range")
        .when(col("price") >= 50, "Budget")
        .otherwise("Economy")
    )
    df_enriched.select("product_name", "price", "price_category").show()
    
    # ==================== STEP 6: Handling Missing Data ====================
    print("\n[STEP 6] Handling Missing Data")
    print("-" * 80)
    
    print("\n6.1 - Drop rows with any NULL values:")
    df_no_nulls = df.dropna()
    print(f"  Original count: {df.count()}")
    print(f"  After dropping nulls: {df_no_nulls.count()}")
    print(f"  Dropped: {df.count() - df_no_nulls.count()} rows")
    
    print("\n6.2 - Fill NULL values:")
    df_filled = df.fillna({
        "product_name": "Unknown Product",
        "price": 0.0,
        "last_updated": "2024-01-01"
    })
    print("  NULL values filled with defaults")
    df_filled.show()
    
    print("\n6.3 - Drop rows with NULL in specific columns:")
    df_price_required = df.dropna(subset=["price"])
    print(f"  Rows with valid price: {df_price_required.count()}")
    
    # ==================== STEP 7: Data Type Conversions ====================
    print("\n[STEP 7] Data Type Conversions")
    print("-" * 80)
    
    print("\n7.1 - Original schema:")
    df.printSchema()
    
    print("\n7.2 - Convert data types:")
    df_typed = df.withColumn("product_id", col("product_id").cast(IntegerType())) \
                 .withColumn("price", col("price").cast(DoubleType())) \
                 .withColumn("stock_quantity", col("stock_quantity").cast(IntegerType()))
    
    print("  After type conversion:")
    df_typed.printSchema()
    
    # ==================== STEP 8: String Operations ====================
    print("\n[STEP 8] String Operations")
    print("-" * 80)
    
    print("\n8.1 - Convert product names to uppercase:")
    df_upper = df.withColumn("product_name_upper", upper(col("product_name")))
    df_upper.select("product_name", "product_name_upper").show(5, truncate=False)
    
    print("\n8.2 - Convert category to lowercase:")
    df_lower = df.withColumn("category_lower", lower(col("category")))
    df_lower.select("category", "category_lower").show(5)
    
    print("\n8.3 - Concatenate columns:")
    df_concat = df.withColumn("product_info", 
                              concat_ws(" - ", col("product_name"), col("category")))
    df_concat.select("product_info").show(5, truncate=False)
    
    # ==================== STEP 9: Sorting and Ordering ====================
    print("\n[STEP 9] Sorting and Ordering")
    print("-" * 80)
    
    print("\n9.1 - Sort by price (ascending):")
    df.orderBy("price").select("product_name", "price").show(5)
    
    print("\n9.2 - Sort by price (descending):")
    df.orderBy(col("price").desc()).select("product_name", "price").show(5)
    
    print("\n9.3 - Sort by multiple columns:")
    df.orderBy(col("category"), col("price").desc()) \
      .select("category", "product_name", "price").show()
    
    # ==================== STEP 10: Grouping and Aggregation ====================
    print("\n[STEP 10] Grouping and Aggregation")
    print("-" * 80)
    
    from pyspark.sql.functions import count, sum as spark_sum, avg, min as spark_min, max as spark_max
    
    print("\n10.1 - Count products by category:")
    df.groupBy("category").count().show()
    
    print("\n10.2 - Statistics by category:")
    df.groupBy("category").agg(
        count("*").alias("product_count"),
        spark_sum("stock_quantity").alias("total_stock"),
        avg("price").alias("avg_price"),
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price")
    ).show()
    
    print("\n10.3 - Total inventory value by category:")
    df_with_value.groupBy("category").agg(
        spark_sum("total_value").alias("total_inventory_value")
    ).orderBy(col("total_inventory_value").desc()).show()
    
    # ==================== STEP 11: Distinct and Deduplication ====================
    print("\n[STEP 11] Distinct and Deduplication")
    print("-" * 80)
    
    print("\n11.1 - Distinct categories:")
    df.select("category").distinct().show()
    
    print("\n11.2 - Count distinct categories:")
    distinct_categories = df.select("category").distinct().count()
    print(f"  Number of distinct categories: {distinct_categories}")
    
    print("\n11.3 - Distinct status values:")
    df.select("status").distinct().show()
    
    # ==================== STEP 12: Renaming Columns ====================
    print("\n[STEP 12] Renaming Columns")
    print("-" * 80)
    
    df_renamed = df.withColumnRenamed("product_name", "name") \
                   .withColumnRenamed("stock_quantity", "qty") \
                   .withColumnRenamed("last_updated", "updated_date")
    
    print("  Original columns:", df.columns)
    print("  Renamed columns:", df_renamed.columns)
    df_renamed.show(3)
    
    # ==================== STEP 13: Sample and Limit ====================
    print("\n[STEP 13] Sample and Limit")
    print("-" * 80)
    
    print("\n13.1 - Limit to 3 rows:")
    df.limit(3).show()
    
    print("\n13.2 - Random sample (50%):")
    df_sample = df.sample(fraction=0.5, seed=42)
    print(f"  Original: {df.count()} rows")
    print(f"  Sample: {df_sample.count()} rows")
    df_sample.show()
    
    # ==================== STEP 14: Save Data ====================
    print("\n[STEP 14] Saving Data to Different Formats")
    print("-" * 80)
    
    output_dir = "output"
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"✓ Created output directory: {output_dir}")
    
    # Save as CSV
    print("\n14.1 - Saving as CSV...")
    df_enriched.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_dir}/products_csv")
    print("  ✓ Saved to CSV format")
    
    # Save as JSON
    print("\n14.2 - Saving as JSON...")
    df_enriched.coalesce(1).write.mode("overwrite") \
        .json(f"{output_dir}/products_json")
    print("  ✓ Saved to JSON format")
    
    # Save as Parquet
    print("\n14.3 - Saving as Parquet...")
    df_enriched.write.mode("overwrite") \
        .parquet(f"{output_dir}/products_parquet")
    print("  ✓ Saved to Parquet format")
    
    # ==================== STEP 15: Reading Data Back ====================
    print("\n[STEP 15] Reading Data from Different Formats")
    print("-" * 80)
    
    print("\n15.1 - Reading CSV:")
    df_from_csv = spark.read.option("header", "true").csv(f"{output_dir}/products_csv")
    print(f"  Loaded {df_from_csv.count()} rows from CSV")
    df_from_csv.show(3)
    
    print("\n15.2 - Reading JSON:")
    df_from_json = spark.read.json(f"{output_dir}/products_json")
    print(f"  Loaded {df_from_json.count()} rows from JSON")
    df_from_json.show(3)
    
    print("\n15.3 - Reading Parquet:")
    df_from_parquet = spark.read.parquet(f"{output_dir}/products_parquet")
    print(f"  Loaded {df_from_parquet.count()} rows from Parquet")
    df_from_parquet.show(3)
    
    # ==================== Application Completed ====================
    print("\n" + "=" * 80)
    print("LAB 2 COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\n✓ All transformations and data operations completed")
    print(f"✓ Output files saved in '{output_dir}/' directory")
    print("\nKey Concepts Covered:")
    print("  • Data loading and exploration")
    print("  • Basic transformations (select, filter, withColumn)")
    print("  • Handling missing data")
    print("  • Data type conversions")
    print("  • String operations")
    print("  • Sorting and aggregation")
    print("  • Saving and reading different file formats")
    
    # Stop SparkSession
    print("\n[FINAL STEP] Stopping SparkSession...")
    spark.stop()
    print("✓ SparkSession stopped successfully")
    print("=" * 80)

if __name__ == "__main__":
    main()
