from pyspark import SparkContext, SparkConf
import os
import sys

# Ensure pyspark uses the project's virtualenv Python for worker and driver
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
venv_python = os.path.join(base_dir, "venv", "Scripts", "python.exe")
if os.path.exists(venv_python):
    os.environ["PYSPARK_PYTHON"] = venv_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

conf = SparkConf().setAppName("Day2-Filter").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("FILTER OPERATIONS")
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
        'order_date': fields[1],
        'status': fields[3],
        'customer_id': int(fields[4]),
        'amount': float(fields[5]),
        'payment_method': fields[6]
    }

orders_rdd = orders_data.map(parse_order)
print(f"Total orders: {orders_rdd.count()}")

# =====================================================
# FILTER 1: Simple Condition
# =====================================================
print("\n[FILTER 1] High-value orders (>$5000)\n")

high_value = orders_rdd.filter(lambda o: o['amount'] > 5000)
print(f"High-value orders: {high_value.count()}")

print("Sample high-value orders:")
for order in high_value.take(5):
    print(f"  Order #{order['order_id']}: ${order['amount']:.2f}")

# =====================================================
# FILTER 2: Multiple Conditions (AND)
# =====================================================
print("\n[FILTER 2] Shipped orders over $2000\n")

shipped_high = orders_rdd.filter(
    lambda o: o['status'] == 'Shipped' and o['amount'] > 2000
)
print(f"Count: {shipped_high.count()}")

print("Sample:")
for order in shipped_high.take(3):
    print(f"  Order #{order['order_id']}: {order['status']} - ${order['amount']:.2f}")

# =====================================================
# FILTER 3: Complex Logic with Function
# =====================================================
print("\n[FILTER 3] Problem orders (On Hold or Cancelled, >$1000)\n")

def is_problem_order(order):
    return (order['status'] in ['On Hold', 'Cancelled']) and \
           (order['amount'] > 1000)

problem_orders = orders_rdd.filter(is_problem_order)
print(f"Problem orders: {problem_orders.count()}")

print("Sample problem orders:")
for order in problem_orders.take(5):
    print(f"  #{order['order_id']}: {order['status']} - ${order['amount']:.2f}")

# =====================================================
# FILTER 4: Date-based Filtering
# =====================================================
print("\n[FILTER 4] Orders from November 2024\n")

from datetime import datetime

def in_november_2024(order):
    try:
        date = datetime.strptime(order['order_date'], '%Y-%m-%d')
        return date.year == 2024 and date.month == 11
    except:
        return False

november_orders = orders_rdd.filter(in_november_2024)
print(f"November 2024 orders: {november_orders.count()}")

# =====================================================
# FILTER 5: Sampling (Random Filtering)
# =====================================================
print("\n[FILTER 5] Sample 10% of orders\n")

sample = orders_rdd.sample(withReplacement=False, fraction=0.1, seed=42)
print(f"Original: {orders_rdd.count()} orders")
print(f"Sample:   {sample.count()} orders (~10%)")

# =====================================================
# FILTER 6: Filter Chaining
# =====================================================
print("\n[FILTER 6] Chaining multiple filters\n")

# Chaining is optimized by Spark
filtered = orders_rdd \
    .filter(lambda o: o['amount'] > 1000) \
    .filter(lambda o: o['status'] == 'Shipped') \
    .filter(lambda o: o['payment_method'] == 'Credit Card')

print(f"After all filters: {filtered.count()} orders")
print("Criteria: Amount > $1000, Status = Shipped, Payment = Credit Card")

# =====================================================
# FILTER 7: Negation (NOT)
# =====================================================
print("\n[FILTER 7] Exclude cancelled orders\n")

not_cancelled = orders_rdd.filter(lambda o: o['status'] != 'Cancelled')
print(f"Non-cancelled orders: {not_cancelled.count()}")

# =====================================================
# PRACTICE EXERCISES
# =====================================================
print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)
print("""
Complete these exercises:

1. Filter orders with amount between $1000 and $5000
2. Find all 'Processing' orders
3. Get orders from customers #1-100
4. Find orders with PayPal payment over $2000
5. Find orders from Q4 2024 (Oct, Nov, Dec)
""")

# =====================================================
# PRACTICE SOLUTIONS
# =====================================================
print("\n" + "=" * 70)
print("PRACTICE SOLUTIONS")
print("=" * 70)

# Solution 1: Amount between $1000 and $5000
range_orders = orders_rdd.filter(lambda o: 1000 <= o['amount'] <= 5000)
print(f"\nSolution 1: {range_orders.count()} orders between $1000-$5000")

# Solution 2: Processing orders
processing = orders_rdd.filter(lambda o: o['status'] == 'Processing')
print(f"Solution 2: {processing.count()} processing orders")

# Solution 3: Customers #1-100
early_customers = orders_rdd.filter(lambda o: 1 <= o['customer_id'] <= 100)
print(f"Solution 3: {early_customers.count()} orders from customers 1-100")

# Solution 4: PayPal over $2000
paypal_high = orders_rdd.filter(
    lambda o: o['payment_method'] == 'PayPal' and o['amount'] > 2000
)
print(f"Solution 4: {paypal_high.count()} PayPal orders over $2000")

# Solution 5: Q4 2024
def in_q4_2024(order):
    date = datetime.strptime(order['order_date'], '%Y-%m-%d')
    return date.year == 2024 and date.month >= 10

q4_orders = orders_rdd.filter(in_q4_2024)
print(f"Solution 5: {q4_orders.count()} orders in Q4 2024")

sc.stop()
