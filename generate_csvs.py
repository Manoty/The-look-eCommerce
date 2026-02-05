import csv
import random
from datetime import datetime, timedelta
import os

# Create raw_data directory
os.makedirs('raw_data', exist_ok=True)

# Set seed for reproducibility
random.seed(42)

# Generate users.csv
print("Generating users.csv...")
users = []
for user_id in range(1, 501):
    users.append({
        'id': user_id,
        'email': f"user{user_id}@example.com",
        'first_name': random.choice(['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana']),
        'last_name': random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']),
        'created_at': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
        'country': random.choice(['US', 'UK', 'CA', 'DE', 'FR']),
        'state': random.choice(['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH'])
    })

with open('raw_data/users.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['id', 'email', 'first_name', 'last_name', 'created_at', 'country', 'state'])
    writer.writeheader()
    writer.writerows(users)

# Generate products.csv
print("Generating products.csv...")
categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
products = []
for product_id in range(1, 101):
    products.append({
        'id': product_id,
        'name': f"Product {product_id}",
        'category': random.choice(categories),
        'price': round(random.uniform(10, 500), 2),
        'cost': round(random.uniform(5, 250), 2),
        'created_at': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
    })

with open('raw_data/products.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['id', 'name', 'category', 'price', 'cost', 'created_at'])
    writer.writeheader()
    writer.writerows(products)

# Generate orders.csv
print("Generating orders.csv...")
orders = []
order_id = 1
for user_id in range(1, 501):
    num_orders = random.randint(0, 10)
    for _ in range(num_orders):
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        orders.append({
            'id': order_id,
            'user_id': user_id,
            'order_date': order_date.isoformat(),
            'status': random.choice(['completed', 'pending', 'cancelled']),
            'total_amount': round(random.uniform(50, 1000), 2)
        })
        order_id += 1

with open('raw_data/orders.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['id', 'user_id', 'order_date', 'status', 'total_amount'])
    writer.writeheader()
    writer.writerows(orders)

# Generate order_items.csv
print("Generating order_items.csv...")
order_items = []
item_id = 1
for order in orders:
    num_items = random.randint(1, 5)
    for _ in range(num_items):
        product = random.choice(products)
        order_items.append({
            'id': item_id,
            'order_id': order['id'],
            'product_id': product['id'],
            'quantity': random.randint(1, 5),
            'unit_price': product['price']
        })
        item_id += 1

with open('raw_data/order_items.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['id', 'order_id', 'product_id', 'quantity', 'unit_price'])
    writer.writeheader()
    writer.writerows(order_items)

# Generate events.csv
print("Generating events.csv...")
event_types = ['page_view', 'add_to_cart', 'purchase', 'search', 'product_view']
events = []
event_id = 1
for user_id in range(1, 501):
    num_events = random.randint(5, 50)
    for _ in range(num_events):
        events.append({
            'id': event_id,
            'user_id': user_id,
            'event_type': random.choice(event_types),
            'event_date': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            'page': random.choice(['/home', '/products', '/cart', '/checkout', '/account'])
        })
        event_id += 1

with open('raw_data/events.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['id', 'user_id', 'event_type', 'event_date', 'page'])
    writer.writeheader()
    writer.writerows(events)

print("\n✓ All CSVs generated successfully!")
print(f"Users: {len(users)}")
print(f"Products: {len(products)}")
print(f"Orders: {len(orders)}")
print(f"Order Items: {len(order_items)}")
print(f"Events: {len(events)}")