from pyspark import SparkContext, SparkConf
import os
import sys
import time

# Ensure pyspark uses the project's virtualenv Python for worker and driver
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
venv_python = os.path.join(base_dir, "venv", "Scripts", "python.exe")
if os.path.exists(venv_python):
    os.environ["PYSPARK_PYTHON"] = venv_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

conf = SparkConf().setAppName("Day2-KeyValue").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("KEY-VALUE RDD OPERATIONS")
print("=" * 70)

# Load orders
orders_path = os.path.join(base_dir, "spark-data", "ecommerce", "orders.csv")
orders = sc.textFile(orders_path)
header = orders.first()
orders_data = orders.filter(lambda line: line != header)

def parse_order(line):
    fields = line.split(',')
    return {
        'order_id': int(fields[0]),
        'status': fields[3],
        'customer_id': int(fields[4]),
        'amount': float(fields[5])
    }

orders_rdd = orders_data.map(parse_order)

# =====================================================
# OPERATION 1: Create Pair RDDs
# =====================================================
print("\n[OPERATION 1] Creating key-value pairs\n")

# Pair: (customer_id, order_amount)
customer_amounts = orders_rdd.map(lambda o: (o['customer_id'], o['amount']))

print("Sample pairs (customer_id, amount):")
for pair in customer_amounts.take(5):
    print(f"  Customer {pair[0]}: ${pair[1]:.2f}")

# =====================================================
# OPERATION 2: keys() and values()
# =====================================================
print("\n[OPERATION 2] Extract keys and values\n")

keys = customer_amounts.keys()
print(f"Sample keys (customer IDs): {keys.take(10)}")

values = customer_amounts.values()
sample_values = values.take(5)
print("Sample values (amounts):")
for v in sample_values:
    print(f"  ${v:.2f}")

# =====================================================
# OPERATION 3: mapValues() - Transform Values Only
# =====================================================
print("\n[OPERATION 3] mapValues() - Add 10% tax\n")

with_tax = customer_amounts.mapValues(lambda amount: amount * 1.1)

print("Sample with 10% tax:")
for customer_id, amount in with_tax.take(5):
    print(f"  Customer {customer_id}: ${amount:.2f}")

# =====================================================
# OPERATION 4: reduceByKey() - Aggregate by Key
# =====================================================
print("\n[OPERATION 4] reduceByKey() - Total per customer\n")

total_per_customer = customer_amounts.reduceByKey(lambda a, b: a + b)

print("Top 10 customers by total spending:")
top_customers = total_per_customer.sortBy(lambda x: x[1], ascending=False).take(10)

for customer_id, total in top_customers:
    print(f"  Customer {customer_id:4d}: ${total:>10,.2f}")

# =====================================================
# OPERATION 5: groupByKey() - Group Values by Key
# =====================================================
print("\n[OPERATION 5] groupByKey() - All orders per customer\n")

grouped = customer_amounts.groupByKey()

print("Sample grouped data:")
for customer_id, amounts in grouped.take(3):
    amounts_list = list(amounts)
    print(f"  Customer {customer_id}: {len(amounts_list)} orders")
    print(f"    Amounts: {[f'${a:.2f}' for a in amounts_list[:5]]}")

print("\n[!] WARNING: groupByKey() shuffles ALL data!")
print("Better alternatives: reduceByKey(), aggregateByKey(), combineByKey()")

# =====================================================
# OPERATION 6: countByKey() - Count per Key
# =====================================================
print("\n[OPERATION 6] countByKey() - Orders per customer\n")

orders_per_customer = customer_amounts.countByKey()
print(f"Total unique customers: {len(orders_per_customer)}")

print("\nSample counts:")
for customer_id in list(orders_per_customer.keys())[:10]:
    count = orders_per_customer[customer_id]
    print(f"  Customer {customer_id:4d}: {count} orders")

# =====================================================
# OPERATION 7: sortByKey() - Sort by Key
# =====================================================
print("\n[OPERATION 7] sortByKey() - Sort by customer ID\n")

sorted_pairs = customer_amounts.sortByKey()

print("First 10 (sorted by customer ID):")
for customer_id, amount in sorted_pairs.take(10):
    print(f"  Customer {customer_id:4d}: ${amount:.2f}")

# =====================================================
# OPERATION 8: aggregateByKey() - Complex Aggregation
# =====================================================
print("\n[OPERATION 8] aggregateByKey() - Stats per customer\n")

# Calculate: (sum, count, min, max) per customer
stats_per_customer = customer_amounts.aggregateByKey(
    (0, 0, float('inf'), float('-inf')),  # Initial: (sum, count, min, max)
    
    # Combine value with accumulator
    lambda acc, val: (
        acc[0] + val,           # sum
        acc[1] + 1,             # count
        min(acc[2], val),       # min
        max(acc[3], val)        # max
    ),
    
    # Combine accumulators
    lambda acc1, acc2: (
        acc1[0] + acc2[0],      # sum
        acc1[1] + acc2[1],      # count
        min(acc1[2], acc2[2]),  # min
        max(acc1[3], acc2[3])   # max
    )
)

print("Customer statistics (top 5 by total spending):")
for customer_id, (total, count, min_val, max_val) in stats_per_customer.sortBy(lambda x: x[1][0], ascending=False).take(5):
    avg = total / count
    print(f"\n  Customer {customer_id}:")
    print(f"    Orders: {count:4d}, Total: ${total:>10,.2f}, Avg: ${avg:>8,.2f}")
    print(f"    Min: ${min_val:>8,.2f}, Max: ${max_val:>8,.2f}")

# =====================================================
# OPERATION 9: combineByKey() - Custom Combine Logic
# =====================================================
print("\n[OPERATION 9] combineByKey() - Calculate average\n")

def create_combiner(value):
    """Create initial combiner from first value"""
    return (value, 1)  # (sum, count)

def merge_value(acc, value):
    """Merge value into accumulator"""
    return (acc[0] + value, acc[1] + 1)

def merge_combiners(acc1, acc2):
    """Merge two accumulators"""
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

average_per_customer = customer_amounts.combineByKey(
    create_combiner,
    merge_value,
    merge_combiners
).mapValues(lambda x: x[0] / x[1])  # Calculate average

print("Top 10 customers by average order value:")
top_avg = average_per_customer.sortBy(lambda x: x[1], ascending=False).take(10)

for customer_id, avg in top_avg:
    print(f"  Customer {customer_id:4d}: ${avg:>8,.2f} average")

# =====================================================
# OPERATION 10: Analysis by Status
# =====================================================
print("\n[OPERATION 10] Analysis by order status\n")

# Pair: (status, amount)
status_amounts = orders_rdd.map(lambda o: (o['status'], o['amount']))

# Total revenue per status
revenue_by_status = status_amounts.reduceByKey(lambda a, b: a + b)

print("Revenue by status:")
for status, revenue in sorted(revenue_by_status.collect()):
    print(f"  {status:15s}: ${revenue:>12,.2f}")

# Count and average per status
stats_by_status = status_amounts.aggregateByKey(
    (0, 0),  # (sum, count)
    lambda acc, val: (acc[0] + val, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
).mapValues(lambda x: {'count': x[1], 'total': x[0], 'average': x[0]/x[1]})

print("\nDetailed statistics by status:")
for status, stats in sorted(stats_by_status.collect()):
    print(f"  {status:15s}: {stats['count']:4d} orders, "
          f"${stats['total']:>12,.2f} total, ${stats['average']:>8,.2f} avg")

# =====================================================
# PERFORMANCE COMPARISON: groupByKey vs reduceByKey
# =====================================================
print("\n" + "=" * 70)
print("PERFORMANCE COMPARISON")
print("=" * 70)

# Create larger dataset for meaningful comparison
large_pairs = sc.parallelize([(i % 100, i) for i in range(100000)])

# Test groupByKey
start = time.time()
result1 = large_pairs.groupByKey().mapValues(sum).count()
time1 = time.time() - start
print(f"groupByKey():  {time1:.4f}s")

# Test reduceByKey
start = time.time()
result2 = large_pairs.reduceByKey(lambda a, b: a + b).count()
time2 = time.time() - start
print(f"reduceByKey(): {time2:.4f}s")

print(f"\n[SUCCESS] reduceByKey is {time1/time2:.2f}x faster!")
print("Reason: reduceByKey combines locally before shuffling")

print("\n" + "=" * 70)
print("QUESTIONS TO ANSWER")
print("=" * 70)
print("""
1. Why is reduceByKey() faster than groupByKey()?
   Answer: reduceByKey() performs local aggregation (combine) on each partition 
   before shuffling data across the network, reducing the amount of data transferred.
   groupByKey() must shuffle ALL values for each key across the network first,
   then aggregate, which transfers much more data.

2. When would you use aggregateByKey() instead of reduceByKey()?
   Answer: Use aggregateByKey() when:
   - The output type differs from input type (e.g., compute average from numbers)
   - You need to track multiple metrics simultaneously (sum, count, min, max)
   - You need different logic for combining within vs across partitions
   - Initial/zero value is needed for the accumulator

3. What does mapValues() preserve that map() doesn't?
   Answer: mapValues() preserves the partitioning of the RDD because it only
   transforms values while keeping keys unchanged. This avoids reshuffling data.
   map() can change both keys and values, so Spark cannot guarantee the existing
   partitioning is still valid, potentially requiring a reshuffle.
""")

sc.stop()
