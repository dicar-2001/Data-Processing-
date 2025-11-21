"""
LAB 3 - Advanced PySpark Operations
Student: [VOTRE NOM]
Date: 2024

This application demonstrates:
- Joins (inner, left, right, outer, cross)
- Window functions and partitioning
- User-Defined Functions (UDFs)
- Advanced aggregations and analytics
- Data partitioning and optimization
- Complex transformations
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

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, concat, upper, lower, 
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    row_number, rank, dense_rank, lag, lead,
    first, last, ntile, percent_rank,
    year, month, dayofmonth, current_date, datediff,
    udf, explode, split, array, struct
)
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField

def main():
    print("=" * 80)
    print("LAB 3 - ADVANCED PYSPARK OPERATIONS")
    print("=" * 80)
    
    # ==================== STEP 1: Initialize SparkSession ====================
    print("\n[STEP 1] Initializing SparkSession...")
    print("-" * 80)
    
    spark = SparkSession.builder \
        .appName("Lab3_AdvancedOperations") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    print("✓ SparkSession created successfully")
    print(f"  Spark Version: {spark.version}")
    
    # ==================== STEP 2: Create Sample Datasets ====================
    print("\n[STEP 2] Creating Sample Datasets")
    print("-" * 80)
    
    # Dataset 1: Customers
    customers_data = [
        (1, "Alice", "Paris", "2020-01-15"),
        (2, "Bob", "London", "2020-03-20"),
        (3, "Charlie", "Berlin", "2020-05-10"),
        (4, "David", "Madrid", "2020-07-25"),
        (5, "Eve", "Paris", "2020-09-30")
    ]
    customers_columns = ["customer_id", "name", "city", "registration_date"]
    customers_df = spark.createDataFrame(customers_data, customers_columns)
    
    # Dataset 2: Orders
    orders_data = [
        (101, 1, "2024-01-10", 250.00, "Delivered"),
        (102, 1, "2024-01-15", 150.00, "Delivered"),
        (103, 2, "2024-01-12", 300.00, "Delivered"),
        (104, 3, "2024-01-20", 450.00, "Shipped"),
        (105, 2, "2024-01-25", 200.00, "Delivered"),
        (106, 1, "2024-02-01", 100.00, "Delivered"),
        (107, 4, "2024-02-05", 500.00, "Pending"),
        (108, 3, "2024-02-10", 350.00, "Delivered"),
        (109, 5, "2024-02-15", 275.00, "Shipped"),
        (110, 2, "2024-02-20", 180.00, "Delivered")
    ]
    orders_columns = ["order_id", "customer_id", "order_date", "amount", "status"]
    orders_df = spark.createDataFrame(orders_data, orders_columns)
    
    # Dataset 3: Products
    products_data = [
        (201, "Laptop", "Electronics", 1200.00),
        (202, "Mouse", "Electronics", 25.00),
        (203, "Keyboard", "Electronics", 75.00),
        (204, "Monitor", "Electronics", 350.00),
        (205, "Desk", "Furniture", 450.00)
    ]
    products_columns = ["product_id", "product_name", "category", "price"]
    products_df = spark.createDataFrame(products_data, products_columns)
    
    print(f"✓ Created 3 datasets:")
    print(f"  - Customers: {customers_df.count()} records")
    print(f"  - Orders: {orders_df.count()} records")
    print(f"  - Products: {products_df.count()} records")
    
    print("\nCustomers Preview:")
    customers_df.show(5, truncate=False)
    
    print("\nOrders Preview:")
    orders_df.show(5, truncate=False)
    
    # ==================== STEP 3: JOINS ====================
    print("\n[STEP 3] JOIN OPERATIONS")
    print("-" * 80)
    
    print("\n3.1 - INNER JOIN (Customers with Orders):")
    inner_join = customers_df.join(
        orders_df,
        customers_df.customer_id == orders_df.customer_id,
        "inner"
    ).select(
        customers_df.customer_id,
        customers_df.name,
        customers_df.city,
        orders_df.order_id,
        orders_df.order_date,
        orders_df.amount,
        orders_df.status
    )
    print(f"  Records: {inner_join.count()}")
    inner_join.show(5)
    
    print("\n3.2 - LEFT JOIN (All Customers, with or without Orders):")
    left_join = customers_df.join(
        orders_df,
        customers_df.customer_id == orders_df.customer_id,
        "left"
    ).select(
        customers_df.customer_id,
        customers_df.name,
        orders_df.order_id,
        orders_df.amount
    )
    print(f"  Records: {left_join.count()}")
    left_join.show()
    
    print("\n3.3 - RIGHT JOIN (All Orders, with Customer info):")
    right_join = orders_df.join(
        customers_df,
        orders_df.customer_id == customers_df.customer_id,
        "right"
    ).select(
        customers_df.customer_id,
        customers_df.name,
        orders_df.order_id,
        orders_df.amount
    )
    print(f"  Records: {right_join.count()}")
    right_join.show(5)
    
    print("\n3.4 - FULL OUTER JOIN:")
    outer_join = customers_df.join(
        orders_df,
        customers_df.customer_id == orders_df.customer_id,
        "outer"
    ).select(
        customers_df.customer_id.alias("cust_id"),
        orders_df.customer_id.alias("order_cust_id"),
        customers_df.name,
        orders_df.order_id
    )
    print(f"  Records: {outer_join.count()}")
    outer_join.show(5)
    
    print("\n3.5 - CROSS JOIN (Cartesian Product - use with caution!):")
    # Using a small subset to demonstrate
    small_customers = customers_df.limit(2)
    small_products = products_df.limit(3)
    cross_join = small_customers.crossJoin(small_products)
    print(f"  Records: {cross_join.count()} (2 customers × 3 products)")
    cross_join.select("name", "product_name", "price").show()
    
    # ==================== STEP 4: WINDOW FUNCTIONS ====================
    print("\n[STEP 4] WINDOW FUNCTIONS")
    print("-" * 80)
    
    print("\n4.1 - ROW NUMBER (Sequential numbering within partition):")
    window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
    orders_with_row_num = orders_df.withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    orders_with_row_num.select(
        "customer_id", "order_id", "order_date", "amount", "row_num"
    ).orderBy("customer_id", "row_num").show()
    
    print("\n4.2 - RANK and DENSE_RANK (Ranking by amount):")
    amount_window = Window.orderBy(col("amount").desc())
    orders_ranked = orders_df.withColumn(
        "rank",
        rank().over(amount_window)
    ).withColumn(
        "dense_rank",
        dense_rank().over(amount_window)
    )
    orders_ranked.select("order_id", "amount", "rank", "dense_rank").show()
    
    print("\n4.3 - LAG and LEAD (Previous and Next values):")
    customer_window = Window.partitionBy("customer_id").orderBy("order_date")
    orders_with_lag_lead = orders_df.withColumn(
        "previous_amount",
        lag("amount", 1).over(customer_window)
    ).withColumn(
        "next_amount",
        lead("amount", 1).over(customer_window)
    )
    orders_with_lag_lead.select(
        "customer_id", "order_date", "amount", "previous_amount", "next_amount"
    ).orderBy("customer_id", "order_date").show()
    
    print("\n4.4 - CUMULATIVE SUM (Running total per customer):")
    cumsum_window = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    orders_cumsum = orders_df.withColumn(
        "cumulative_amount",
        spark_sum("amount").over(cumsum_window)
    )
    orders_cumsum.select(
        "customer_id", "order_date", "amount", "cumulative_amount"
    ).orderBy("customer_id", "order_date").show()
    
    print("\n4.5 - MOVING AVERAGE (3-order rolling average):")
    moving_avg_window = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(-2, 0)
    orders_moving_avg = orders_df.withColumn(
        "moving_avg",
        avg("amount").over(moving_avg_window)
    )
    orders_moving_avg.select(
        "customer_id", "order_date", "amount", "moving_avg"
    ).orderBy("customer_id", "order_date").show()
    
    print("\n4.6 - NTILE (Divide into quartiles):")
    ntile_window = Window.orderBy("amount")
    orders_ntile = orders_df.withColumn(
        "quartile",
        ntile(4).over(ntile_window)
    )
    orders_ntile.select("order_id", "amount", "quartile").orderBy("amount").show()
    
    print("\n4.7 - FIRST and LAST values in window:")
    first_last_window = Window.partitionBy("customer_id").orderBy("order_date")
    orders_first_last = orders_df.withColumn(
        "first_order_amount",
        first("amount").over(first_last_window)
    ).withColumn(
        "last_order_amount",
        last("amount").over(first_last_window)
    )
    orders_first_last.select(
        "customer_id", "order_date", "amount", "first_order_amount", "last_order_amount"
    ).orderBy("customer_id", "order_date").show()
    
    # ==================== STEP 5: USER-DEFINED FUNCTIONS (UDFs) ====================
    print("\n[STEP 5] USER-DEFINED FUNCTIONS (UDFs)")
    print("-" * 80)
    
    print("\n5.1 - Simple UDF (Categorize amount):")
    # Define Python function
    def categorize_amount(amount):
        if amount is None:
            return "Unknown"
        elif amount < 100:
            return "Small"
        elif amount < 300:
            return "Medium"
        else:
            return "Large"
    
    # Register as UDF
    categorize_udf = udf(categorize_amount, StringType())
    
    # Apply UDF
    orders_categorized = orders_df.withColumn(
        "amount_category",
        categorize_udf(col("amount"))
    )
    orders_categorized.select("order_id", "amount", "amount_category").show()
    
    print("\n5.2 - UDF with Multiple Parameters:")
    def calculate_discount(amount, status):
        if status == "Pending":
            return amount * 0.9  # 10% discount
        elif status == "Shipped":
            return amount * 0.95  # 5% discount
        else:
            return amount  # No discount
    
    discount_udf = udf(calculate_discount, DoubleType())
    
    orders_with_discount = orders_df.withColumn(
        "discounted_amount",
        discount_udf(col("amount"), col("status"))
    )
    orders_with_discount.select(
        "order_id", "amount", "status", "discounted_amount"
    ).show()
    
    print("\n5.3 - UDF for String Manipulation:")
    def format_customer_name(name):
        if name is None:
            return "N/A"
        return f"Customer: {name.upper()}"
    
    format_name_udf = udf(format_customer_name, StringType())
    
    customers_formatted = customers_df.withColumn(
        "formatted_name",
        format_name_udf(col("name"))
    )
    customers_formatted.select("customer_id", "name", "formatted_name").show()
    
    # ==================== STEP 6: ADVANCED AGGREGATIONS ====================
    print("\n[STEP 6] ADVANCED AGGREGATIONS")
    print("-" * 80)
    
    print("\n6.1 - Multiple Aggregations per Group:")
    customer_stats = orders_df.groupBy("customer_id").agg(
        count("order_id").alias("total_orders"),
        spark_sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_amount"),
        spark_min("amount").alias("min_order"),
        spark_max("amount").alias("max_order")
    ).orderBy(col("total_spent").desc())
    customer_stats.show()
    
    print("\n6.2 - Aggregation with Multiple Grouping Columns:")
    status_stats = orders_df.groupBy("customer_id", "status").agg(
        count("order_id").alias("order_count"),
        spark_sum("amount").alias("total_amount")
    ).orderBy("customer_id", "status")
    status_stats.show()
    
    print("\n6.3 - Conditional Aggregation:")
    conditional_agg = orders_df.groupBy("customer_id").agg(
        count(when(col("status") == "Delivered", 1)).alias("delivered_orders"),
        count(when(col("status") == "Pending", 1)).alias("pending_orders"),
        spark_sum(when(col("status") == "Delivered", col("amount")).otherwise(0)).alias("delivered_amount")
    )
    conditional_agg.show()
    
    print("\n6.4 - Pivot Table (Status by Customer):")
    pivot_df = orders_df.groupBy("customer_id").pivot("status").agg(
        count("order_id")
    ).fillna(0)
    pivot_df.show()
    
    # ==================== STEP 7: COMPLEX TRANSFORMATIONS ====================
    print("\n[STEP 7] COMPLEX TRANSFORMATIONS")
    print("-" * 80)
    
    print("\n7.1 - Nested When-Otherwise Chains:")
    orders_priority = orders_df.withColumn(
        "priority",
        when(col("amount") >= 400, "High")
        .when(col("amount") >= 200, "Medium")
        .otherwise("Low")
    ).withColumn(
        "processing_time",
        when(col("priority") == "High", "1 day")
        .when(col("priority") == "Medium", "3 days")
        .otherwise("5 days")
    )
    orders_priority.select("order_id", "amount", "priority", "processing_time").show()
    
    print("\n7.2 - Array Operations:")
    # Create array column
    customers_with_tags = customers_df.withColumn(
        "tags",
        array(lit("Premium"), col("city"), lit("Active"))
    )
    customers_with_tags.select("customer_id", "name", "tags").show(truncate=False)
    
    # Explode array
    print("\n  Exploding array into rows:")
    exploded = customers_with_tags.select("customer_id", "name", explode("tags").alias("tag"))
    exploded.show()
    
    print("\n7.3 - Struct (Nested Structure):")
    customers_with_struct = customers_df.withColumn(
        "profile",
        struct(
            col("name").alias("full_name"),
            col("city").alias("location"),
            lit("Active").alias("account_status")
        )
    )
    customers_with_struct.select("customer_id", "profile").show(truncate=False)
    
    # Access struct fields
    print("\n  Accessing struct fields:")
    customers_with_struct.select(
        "customer_id",
        col("profile.full_name"),
        col("profile.location")
    ).show()
    
    # ==================== STEP 8: SELF-JOIN ====================
    print("\n[STEP 8] SELF-JOIN (Find Customer Pairs from Same City)")
    print("-" * 80)
    
    customers_alias1 = customers_df.alias("c1")
    customers_alias2 = customers_df.alias("c2")
    
    same_city_pairs = customers_alias1.join(
        customers_alias2,
        (col("c1.city") == col("c2.city")) & (col("c1.customer_id") < col("c2.customer_id")),
        "inner"
    ).select(
        col("c1.name").alias("customer1"),
        col("c2.name").alias("customer2"),
        col("c1.city")
    )
    same_city_pairs.show()
    
    # ==================== STEP 9: UNION and INTERSECTION ====================
    print("\n[STEP 9] UNION AND INTERSECTION")
    print("-" * 80)
    
    # Create two subsets
    high_value_customers = orders_df.filter(col("amount") >= 300).select("customer_id").distinct()
    frequent_customers = orders_df.groupBy("customer_id").agg(
        count("order_id").alias("order_count")
    ).filter(col("order_count") >= 3).select("customer_id")
    
    print("\n9.1 - UNION (High Value OR Frequent):")
    union_customers = high_value_customers.union(frequent_customers).distinct()
    print(f"  Total unique customers: {union_customers.count()}")
    union_customers.show()
    
    print("\n9.2 - INTERSECT (High Value AND Frequent):")
    intersect_customers = high_value_customers.intersect(frequent_customers)
    print(f"  VIP customers: {intersect_customers.count()}")
    intersect_customers.show()
    
    print("\n9.3 - EXCEPT (High Value but NOT Frequent):")
    except_customers = high_value_customers.subtract(frequent_customers)
    print(f"  Occasional high spenders: {except_customers.count()}")
    except_customers.show()
    
    # ==================== STEP 10: ADVANCED FILTERING ====================
    print("\n[STEP 10] ADVANCED FILTERING")
    print("-" * 80)
    
    print("\n10.1 - Filter with IN clause:")
    selected_customers = orders_df.filter(col("customer_id").isin([1, 2, 3]))
    selected_customers.select("order_id", "customer_id", "amount").show()
    
    print("\n10.2 - Filter with BETWEEN:")
    medium_orders = orders_df.filter(col("amount").between(200, 400))
    medium_orders.select("order_id", "amount").show()
    
    print("\n10.3 - Filter with LIKE:")
    paris_customers = customers_df.filter(col("city").like("Pa%"))
    paris_customers.show()
    
    print("\n10.4 - Filter with Multiple Conditions:")
    complex_filter = orders_df.filter(
        (col("amount") > 200) & 
        (col("status").isin(["Delivered", "Shipped"])) &
        (col("order_date") >= "2024-02-01")
    )
    complex_filter.show()
    
    # ==================== STEP 11: SUBQUERIES (Using Join) ====================
    print("\n[STEP 11] SUBQUERY PATTERN (Customers with Above-Average Orders)")
    print("-" * 80)
    
    # Calculate average order amount
    avg_amount = orders_df.select(avg("amount")).first()[0]
    print(f"\n  Average order amount: ${avg_amount:.2f}")
    
    # Find customers with orders above average
    above_avg_orders = orders_df.filter(col("amount") > avg_amount)
    customers_above_avg = above_avg_orders.join(
        customers_df,
        "customer_id"
    ).select(
        customers_df.customer_id,
        customers_df.name,
        above_avg_orders.order_id,
        above_avg_orders.amount
    ).distinct()
    
    print(f"\n  Customers with above-average orders:")
    customers_above_avg.show()
    
    # ==================== STEP 12: CASE STUDY - Customer Segmentation ====================
    print("\n[STEP 12] CASE STUDY - Customer Segmentation Analysis")
    print("-" * 80)
    
    # Calculate customer metrics
    customer_metrics = orders_df.groupBy("customer_id").agg(
        count("order_id").alias("total_orders"),
        spark_sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value"),
        spark_max("order_date").alias("last_order_date")
    )
    
    # Join with customer details
    customer_profile = customer_metrics.join(customers_df, "customer_id")
    
    # Segment customers
    customer_segments = customer_profile.withColumn(
        "segment",
        when((col("total_spent") >= 500) & (col("total_orders") >= 3), "VIP")
        .when((col("total_spent") >= 300) & (col("total_orders") >= 2), "Premium")
        .when(col("total_orders") >= 2, "Regular")
        .otherwise("New")
    ).withColumn(
        "lifetime_value_score",
        (col("total_spent") * 0.7 + col("total_orders") * 50)
    )
    
    print("\n12.1 - Customer Segmentation Summary:")
    customer_segments.select(
        "customer_id", "name", "city", 
        "total_orders", "total_spent", "avg_order_value", 
        "segment", "lifetime_value_score"
    ).orderBy(col("lifetime_value_score").desc()).show()
    
    print("\n12.2 - Segment Distribution:")
    segment_distribution = customer_segments.groupBy("segment").agg(
        count("customer_id").alias("customer_count"),
        avg("total_spent").alias("avg_spent"),
        avg("total_orders").alias("avg_orders")
    ).orderBy(col("avg_spent").desc())
    segment_distribution.show()
    
    print("\n12.3 - Top 3 Customers by Segment:")
    segment_window = Window.partitionBy("segment").orderBy(col("lifetime_value_score").desc())
    top_by_segment = customer_segments.withColumn(
        "rank_in_segment",
        row_number().over(segment_window)
    ).filter(col("rank_in_segment") <= 3)
    
    top_by_segment.select(
        "segment", "name", "total_spent", "total_orders", "rank_in_segment"
    ).orderBy("segment", "rank_in_segment").show()
    
    # ==================== STEP 13: DATA QUALITY CHECKS ====================
    print("\n[STEP 13] DATA QUALITY CHECKS")
    print("-" * 80)
    
    print("\n13.1 - Check for Duplicates:")
    duplicate_orders = orders_df.groupBy("order_id").count().filter(col("count") > 1)
    print(f"  Duplicate order_ids: {duplicate_orders.count()}")
    
    print("\n13.2 - Check for Missing Values:")
    from pyspark.sql.functions import sum as spark_sum, when, col
    null_counts = orders_df.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in orders_df.columns
    ])
    print("  NULL value counts:")
    null_counts.show()
    
    print("\n13.3 - Data Type Validation:")
    print("  Schema validation:")
    orders_df.printSchema()
    
    # ==================== Application Completed ====================
    print("\n" + "=" * 80)
    print("LAB 3 COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\n✓ All advanced operations completed")
    print("\nKey Concepts Covered:")
    print("  • Different types of joins (inner, left, right, outer, cross)")
    print("  • Window functions (row_number, rank, lag, lead, cumulative)")
    print("  • User-Defined Functions (UDFs)")
    print("  • Advanced aggregations and pivot tables")
    print("  • Complex transformations (arrays, structs)")
    print("  • Set operations (union, intersect, except)")
    print("  • Customer segmentation analysis")
    print("  • Data quality validation")
    
    # Stop SparkSession
    print("\n[FINAL STEP] Stopping SparkSession...")
    spark.stop()
    print("✓ SparkSession stopped successfully")
    print("=" * 80)

if __name__ == "__main__":
    main()
