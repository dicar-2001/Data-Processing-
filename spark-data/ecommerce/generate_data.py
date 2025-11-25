"""
Data Generation Script for LAB 2 - E-Commerce Dataset
Generates synthetic CSV data for customers, products, and orders
"""

import csv
import random
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

# ============================================================================
# CUSTOMERS DATA GENERATION (1000 records)
# ============================================================================

def generate_customers(num_customers=1000):
    """Generate synthetic customer data"""
    
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", 
                   "James", "Mary", "William", "Patricia", "Richard", "Jennifer", "Charles",
                   "Linda", "Thomas", "Barbara", "Christopher", "Susan", "Daniel", "Jessica",
                   "Matthew", "Karen", "Anthony", "Nancy", "Mark", "Betty", "Donald", "Helen"]
    
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", 
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", 
                  "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
                  "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", 
                  "Ramirez", "Lewis", "Robinson"]
    
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
              "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
              "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis",
              "Seattle", "Denver", "Boston", "London", "Paris", "Berlin", "Madrid", 
              "Rome", "Tokyo", "Beijing", "Sydney", "Toronto", "Mumbai"]
    
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "NC", "WA", 
              "CO", "MA", "ON", "BC", "QC", None, None, None]
    
    countries = ["USA", "USA", "USA", "USA", "USA", "Canada", "Canada", 
                 "UK", "France", "Germany", "Spain", "Italy", "Japan", 
                 "China", "Australia", "India", "Brazil", "Mexico"]
    
    segments = ["Enterprise", "SMB", "Individual", "Government"]
    
    customers = []
    
    for i in range(1, num_customers + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        country = random.choice(countries)
        state = random.choice(states) if country in ["USA", "Canada"] else None
        
        customer = {
            "customerNumber": i,
            "customerName": f"{first_name} {last_name} {random.choice(['Inc', 'LLC', 'Corp', 'Co', 'Ltd'])}",
            "contactFirstName": first_name,
            "contactLastName": last_name,
            "phone": f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            "addressLine1": f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Maple', 'Cedar', 'Elm', 'Washington', 'Park'])} {random.choice(['St', 'Ave', 'Blvd', 'Rd', 'Dr'])}",
            "city": random.choice(cities),
            "state": state if state else "",
            "country": country,
            "creditLimit": round(random.uniform(1000, 100000), 2),
            "customerSegment": random.choice(segments)
        }
        customers.append(customer)
    
    return customers


# ============================================================================
# PRODUCTS DATA GENERATION (100 records)
# ============================================================================

def generate_products(num_products=100):
    """Generate synthetic product data"""
    
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports & Outdoors", 
                  "Books", "Toys & Games", "Beauty & Health", "Automotive", 
                  "Food & Beverage", "Office Supplies"]
    
    product_prefixes = {
        "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Camera", "Monitor", "Keyboard", "Mouse"],
        "Clothing": ["T-Shirt", "Jeans", "Dress", "Jacket", "Sweater", "Shoes", "Hat", "Scarf"],
        "Home & Garden": ["Lamp", "Chair", "Table", "Sofa", "Bed", "Plant", "Rug", "Curtain"],
        "Sports & Outdoors": ["Bike", "Tent", "Ball", "Racket", "Weights", "Mat", "Shoes", "Backpack"],
        "Books": ["Novel", "Textbook", "Magazine", "Comic", "Biography", "Cookbook", "Guide", "Dictionary"],
        "Toys & Games": ["Puzzle", "Doll", "Car", "Board Game", "LEGO", "Action Figure", "Plush", "Robot"],
        "Beauty & Health": ["Shampoo", "Lotion", "Perfume", "Makeup", "Soap", "Vitamins", "Cream", "Oil"],
        "Automotive": ["Tire", "Battery", "Oil", "Filter", "Wiper", "Light", "Mirror", "Cover"],
        "Food & Beverage": ["Coffee", "Tea", "Snack", "Juice", "Water", "Candy", "Sauce", "Spice"],
        "Office Supplies": ["Pen", "Notebook", "Stapler", "Folder", "Calculator", "Tape", "Paper", "Organizer"]
    }
    
    brands = ["TechPro", "StyleMax", "HomeComfort", "SportElite", "ReadWell", 
              "PlayFun", "BeautyPlus", "AutoCare", "TasteGood", "WorkSmart"]
    
    products = []
    
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        prefix = random.choice(product_prefixes[category])
        brand = random.choice(brands)
        
        buy_price = round(random.uniform(10, 500), 2)
        msrp = round(buy_price * random.uniform(1.5, 3.0), 2)
        
        product = {
            "productCode": f"P{i:04d}",
            "productName": f"{brand} {prefix} {random.choice(['Pro', 'Plus', 'Max', 'Ultra', 'Premium', 'Classic', 'Deluxe', 'Standard'])}",
            "productCategory": category,
            "quantityInStock": random.randint(0, 500),
            "buyPrice": buy_price,
            "MSRP": msrp
        }
        products.append(product)
    
    return products


# ============================================================================
# ORDERS DATA GENERATION (5000 records)
# ============================================================================

def generate_orders(num_orders=5000, num_customers=1000, num_products=100):
    """Generate synthetic order data"""
    
    statuses = ["Shipped", "Delivered", "Processing", "Cancelled", "Pending"]
    payment_methods = ["Credit Card", "PayPal", "Debit Card", "Bank Transfer", "Cash on Delivery"]
    
    orders = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    for i in range(1, num_orders + 1):
        order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        required_date = order_date + timedelta(days=random.randint(3, 30))
        
        order = {
            "orderNumber": i,
            "orderDate": order_date.strftime("%Y-%m-%d"),
            "requiredDate": required_date.strftime("%Y-%m-%d"),
            "status": random.choice(statuses),
            "customerNumber": random.randint(1, num_customers),
            "totalAmount": round(random.uniform(50, 5000), 2),
            "paymentMethod": random.choice(payment_methods)
        }
        orders.append(order)
    
    return orders


# ============================================================================
# WRITE CSV FILES
# ============================================================================

def write_csv(filename, data, fieldnames):
    """Write data to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Created {filename} with {len(data)} records")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("DATA GENERATION FOR LAB 2 - E-COMMERCE DATASET")
    print("=" * 80)
    
    print("\nGenerating synthetic data...")
    
    # Generate data
    customers = generate_customers(1000)
    products = generate_products(100)
    orders = generate_orders(5000, 1000, 100)
    
    # Write CSV files
    print("\nWriting CSV files...")
    
    customer_fields = ["customerNumber", "customerName", "contactFirstName", "contactLastName",
                       "phone", "addressLine1", "city", "state", "country", 
                       "creditLimit", "customerSegment"]
    write_csv("customers.csv", customers, customer_fields)
    
    product_fields = ["productCode", "productName", "productCategory", 
                      "quantityInStock", "buyPrice", "MSRP"]
    write_csv("products.csv", products, product_fields)
    
    order_fields = ["orderNumber", "orderDate", "requiredDate", "status", 
                    "customerNumber", "totalAmount", "paymentMethod"]
    write_csv("orders.csv", orders, order_fields)
    
    print("\n" + "=" * 80)
    print("DATA GENERATION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\nGenerated files:")
    print("  • customers.csv  (1000 records)")
    print("  • products.csv   (100 records)")
    print("  • orders.csv     (5000 records)")
    print("\nYou can now run: python lab2_explore_data.py")
    print("=" * 80)
