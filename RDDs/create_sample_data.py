from pyspark import SparkContext, SparkConf
import os
import csv
import sys


# Force Spark to use the correct Python inside your virtualenv
os.environ["PYSPARK_PYTHON"] = r"C:\Users\CHADIA\data-engineering-course\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\CHADIA\data-engineering-course\venv\Scripts\python.exe"


conf = SparkConf().setMaster("local[*]").setAppName("SampleData")
sc = SparkContext(conf=conf)

print("=" * 70)
print("MAP TRANSFORMATIONS")
print("=" * 70)

# Load e-commerce data
# Resolve data path relative to the repository root (avoid cwd issues)
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
customers_path = os.path.join(base_dir, "spark-data", "ecommerce", "customers.csv")

# Check the CSV exists and provide a helpful error if not
if not os.path.exists(customers_path):
    print(f"ERROR: expected customers CSV at: {customers_path}\nPlease run `python spark-data\\ecommerce\\generate_data.py` to create sample data.")
    sc.stop()
    sys.exit(1)

customers = sc.textFile(customers_path)

# Skip header
header = customers.first()
customers_data = customers.filter(lambda line: line != header)

print(f"Total customer records: {customers_data.count()}")

# =====================================================
# OPERATION 1: map() - Basic Transformation
# =====================================================
print("\n[OPERATION 1] map() - Parse CSV lines\n")

def parse_customer(line):
    """Parse CSV line into dictionary"""
    fields = next(csv.reader([line]))
    return {
        'id': int(fields[0]),
        'name': fields[1],
        'first_name': fields[2],
        'last_name': fields[3],
        'phone': fields[4],
        'address': fields[5],
        'city': fields[6],
        'country': fields[8],
        'credit_limit': float(fields[9]) if fields[9] else 0.0,
        'segment': fields[10]
    }

customers_parsed = customers_data.map(parse_customer)

print("First 3 parsed customers:")
for customer in customers_parsed.take(3):
    print(f" {customer['name']} from {customer['city']}, {customer['country']}")

# =====================================================
# OPERATION 2: map() - Extract Specific Fields
# =====================================================
print("\n[OPERATION 2] map() - Extract names only\n")

names = customers_parsed.map(lambda c: c['name'])

print("First 10 customer names:")
for name in names.take(10):
    print(f" - {name}")

# =====================================================
# OPERATION 3: map() - Compute Derived Values
# =====================================================
print("\n[OPERATION 3] map() - Create full name and email\n")

def create_profile(customer):
    full_name = f"{customer['first_name']} {customer['last_name']}"
    email = f"{customer['first_name'].lower()}.{customer['last_name'].lower()}@example.com"
    return {
        'id': customer['id'],
        'full_name': full_name,
        'email': email,
        'segment': customer['segment']
    }

profiles = customers_parsed.map(create_profile)

print("First 5 customer profiles:")
for profile in profiles.take(5):
    print(f" {profile['full_name']} <{profile['email']}> [{profile['segment']}]")

# =====================================================
# OPERATION 4: map() - Complex Transformations
# =====================================================
print("\n[OPERATION 4] map() - Categorize credit limits\n")

def categorize_customer(customer):
    credit = customer['credit_limit']
    if credit >= 75000:
        tier = "Platinum"
    elif credit >= 50000:
        tier = "Gold"
    elif credit >= 25000:
        tier = "Silver"
    else:
        tier = "Bronze"

    return (customer['name'], customer['segment'], credit, tier)

categorized = customers_parsed.map(categorize_customer)

print("Customer tiers (first 10):")
for name, segment, credit, tier in categorized.take(10):
    print(f" {name:30s} | {segment:10s} | ${credit:8.0f} | {tier}")

# =====================================================
# OPERATION 5: map() vs mapPartitions() performance
# =====================================================
print("\n[OPERATION 5] map() vs mapPartitions() performance\n")

import time

# Using map (called for each element)
start = time.time()
result1 = customers_parsed.map(lambda c: c['credit_limit'] * 1.1).reduce(lambda a, b: a + b)
time1 = time.time() - start
print(f"map(): {time1:.4f}s | Result: ${result1:,.2f}")

# Using mapPartitions (called once per partition)
def process_partition(iterator):
    multiplier = 1.1
    for customer in iterator:
        yield customer['credit_limit'] * multiplier

start = time.time()
result2 = customers_parsed.mapPartitions(process_partition).reduce(lambda a, b: a + b)
time2 = time.time() - start
print(f"mapPartitions(): {time2:.4f}s | Result: ${result2:,.2f}")

print(f"Difference: {abs(time1 - time2):.4f}s")

# =====================================================
# PRACTICE EXERCISES
# =====================================================

print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)

print("""
Complete these exercises in Part C:
1. Extract all customers from USA
2. Create a tuple of (city, country) for each customer
3. Calculate average credit limit by segment
4. Create customer ID string formatted as "CUST-00001"
5. Identify customers with credit limit > $50,000

Run the script:
python lab2_map_operations.py

Questions to Answer:
1. What does the map() transformation return?
2. When should you use mapPartitions() instead of map()?
3. Can map() change the number of elements in the RDD?
""")

sc.stop()
