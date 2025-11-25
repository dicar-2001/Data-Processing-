import csv
import random
from datetime import datetime, timedelta
import os

# Create sample orders
orders_data = []
statuses = ['Shipped', 'Processing', 'On Hold', 'Cancelled']
payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash']
start_date = datetime(2024, 1, 1)

for i in range(1, 5001):  # 5000 orders
    order_date = start_date + timedelta(days=random.randint(0, 330))
    customer_id = random.randint(1, 1000)
    amount = round(random.uniform(100, 10000), 2)
    status = random.choice(statuses)
    payment = random.choice(payment_methods)

    orders_data.append([
        i,  # orderNumber
        order_date.strftime('%Y-%m-%d'),  # orderDate
        order_date.strftime('%Y-%m-%d'),  # requiredDate (same for simplicity)
        status,  # status
        customer_id,  # customerNumber
        amount,  # amount
        payment  # paymentMethod
    ])

# Ensure directory exists
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
orders_dir = os.path.join(base_dir, "spark-data", "ecommerce")
os.makedirs(orders_dir, exist_ok=True)

# Write to CSV
orders_path = os.path.join(orders_dir, "orders.csv")
with open(orders_path, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['orderNumber', 'orderDate', 'requiredDate', 'status',
                     'customerNumber', 'amount', 'paymentMethod'])
    writer.writerows(orders_data)

print("Orders data created successfully!")
print(f"Created {len(orders_data)} order records")
print(f"File saved at: {orders_path}")
