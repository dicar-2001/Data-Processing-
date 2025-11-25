import csv
import random
import datetime
from faker import Faker

fake = Faker()

# -------------------- Generate Customers --------------------
customers = []
for i in range(1000):
    customers.append([
        i + 1,                            # customerNumber
        fake.company(),                   # customerName
        fake.first_name(),                # contactFirstName
        fake.last_name(),                 # contactLastName
        fake.phone_number(),              # phone
        fake.street_address(),            # addressLine1
        fake.city(),                      # city
        fake.state(),                     # state
        fake.country(),                   # country
        round(random.uniform(1000, 50000), 2),  # creditLimit
        random.choice(["Small Business", "Enterprise", "Individual"])  # customerSegment
    ])

# -------------------- Generate Products --------------------
products = []
categories = ["Electronics", "Furniture", "Office", "Outdoor", "Toys"]
for i in range(100):
    products.append([
        i + 1,                            # productCode
        fake.word().capitalize(),         # productName
        random.choice(categories),        # productCategory
        random.randint(10, 500),          # quantityInStock
        round(random.uniform(10, 200), 2),  # buyPrice
        round(random.uniform(200, 500), 2)  # MSRP
    ])

# -------------------- Generate Orders --------------------
orders = []
for i in range(5000):
    order_date = fake.date_between(start_date="-1y", end_date="today")
    required = order_date + datetime.timedelta(days=random.randint(3, 10))

    orders.append([
        i + 1,                            # orderNumber
        str(order_date),                  # orderDate (YYYY-MM-DD)
        str(required),                    # requiredDate
        random.choice(["Shipped", "Pending", "Cancelled", "Delivered"]),  # status
        random.randint(1, 1000),          # customerNumber
        round(random.uniform(50, 3000), 2),  # totalAmount
        random.choice(["Credit Card", "PayPal", "Bank Transfer", "Cash"]) # paymentMethod
    ])

# -------------------- Write CSV Files --------------------
with open("customers.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "customerNumber","customerName","contactFirstName","contactLastName",
        "phone","addressLine1","city","state","country","creditLimit","customerSegment"
    ])
    writer.writerows(customers)

with open("products.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "productCode","productName","productCategory","quantityInStock",
        "buyPrice","MSRP"
    ])
    writer.writerows(products)

with open("orders.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "orderNumber","orderDate","requiredDate","status","customerNumber",
        "totalAmount","paymentMethod"
    ])
    writer.writerows(orders)

print("CSV files generated successfully!")
