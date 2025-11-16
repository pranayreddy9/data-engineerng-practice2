import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_PATH, exist_ok=True)

# 1. customers
customers = pd.DataFrame({
    "customer_id": [1, 2, 3],
    "first_name": ["John", "Emma", "Raj"],
    "last_name": ["Doe", "Stone", "Kumar"],
    "email": ["john@example.com", "emma@example.com", "raj@example.com"],
    "country": ["USA", "Canada", "India"]
})
customers.to_csv(os.path.join(RAW_PATH, "customers.csv"), index=False)

# 2. products
products = pd.DataFrame({
    "product_id": [101, 102, 103],
    "product_name": ["Laptop", "Headphones", "Keyboard"],
    "category": ["Electronics", "Electronics", "Electronics"],
    "price": [1200, 150, 70]
})
products.to_csv(os.path.join(RAW_PATH, "products.csv"), index=False)

# 3. orders
orders = pd.DataFrame({
    "order_id": [1001, 1002, 1003],
    "customer_id": [1, 2, 3],
    "order_date": ["2024-01-05", "2024-01-06", "2024-01-07"],
})
orders.to_csv(os.path.join(RAW_PATH, "orders.csv"), index=False)

# 4. order_items
order_items = pd.DataFrame({
    "order_id": [1001, 1001, 1002],
    "product_id": [101, 102, 103],
    "quantity": [1, 2, 1]
})
order_items.to_csv(os.path.join(RAW_PATH, "order_items.csv"), index=False)

# 5. payments
payments = pd.DataFrame({
    "payment_id": [501, 502, 503],
    "order_id": [1001, 1002, 1003],
    "amount": [1500, 150, 1200],
    "method": ["Card", "Card", "Cash"]
})
payments.to_csv(os.path.join(RAW_PATH, "payments.csv"), index=False)

# 6. inventory
inventory = pd.DataFrame({
    "product_id": [101, 102, 103],
    "stock_qty": [10, 50, 100]
})
inventory.to_csv(os.path.join(RAW_PATH, "inventory.csv"), index=False)

# 7. shipments
shipments = pd.DataFrame({
    "shipment_id": [1, 2, 3],
    "order_id": [1001, 1002, 1003],
    "status": ["Delivered", "Shipped", "Pending"]
})
shipments.to_csv(os.path.join(RAW_PATH, "shipments.csv"), index=False)

# 8. returns
returns = pd.DataFrame({
    "return_id": [1, 2],
    "order_id": [1002, 1003],
    "reason": ["Damaged", "Wrong Item"]
})
returns.to_csv(os.path.join(RAW_PATH, "returns.csv"), index=False)

print("Raw data generated.")
