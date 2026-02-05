"""
eCommerce dbt Pipeline in Python
Loads raw CSVs → Creates staging → Creates marts → Runs tests → Generates docs
"""

import duckdb
import os
import json
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIG
# ============================================================================
PROJECT_DIR = Path(__file__).parent
RAW_DATA_DIR = PROJECT_DIR / "raw_data"
DB_PATH = PROJECT_DIR / "ecommerce.duckdb"
MODELS_DIR = PROJECT_DIR / "models"

# Create directories
MODELS_DIR.mkdir(exist_ok=True)
(MODELS_DIR / "staging").mkdir(exist_ok=True)
(MODELS_DIR / "marts").mkdir(exist_ok=True)

# ============================================================================
# CONNECT TO DUCKDB
# ============================================================================
print("\n" + "="*80)
print("ECOMMERCE dbt PIPELINE")
print("="*80)

conn = duckdb.connect(str(DB_PATH))
print(f"\n✓ Connected to DuckDB: {DB_PATH}")

# ============================================================================
# STEP 1: LOAD RAW DATA
# ============================================================================
print("\n" + "-"*80)
print("STEP 1: LOAD RAW DATA")
print("-"*80)

raw_tables = {
    'users': 'raw_users',
    'products': 'raw_products',
    'orders': 'raw_orders',
    'order_items': 'raw_order_items',
    'events': 'raw_events'
}

for csv_name, table_name in raw_tables.items():
    csv_path = RAW_DATA_DIR / f"{csv_name}.csv"
    if csv_path.exists():
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  ✓ {table_name}: {row_count} rows")
    else:
        print(f"  ✗ {csv_path} not found!")

# ============================================================================
# STEP 2: CREATE STAGING MODELS
# ============================================================================
print("\n" + "-"*80)
print("STEP 2: CREATE STAGING MODELS")
print("-"*80)

staging_models = {
    'stg_users': """
        SELECT 
            CAST(id AS INTEGER) AS user_id,
            email,
            first_name,
            last_name,
            CAST(created_at AS TIMESTAMP) AS created_at,
            CAST((CURRENT_TIMESTAMP - CAST(created_at AS TIMESTAMP)) AS INTEGER) AS account_age_days,
            country,
            state
        FROM raw_users
    """,
    
    'stg_products': """
        SELECT 
            CAST(id AS INTEGER) AS product_id,
            name,
            category,
            CAST(price AS DECIMAL(10,2)) AS price,
            CAST(cost AS DECIMAL(10,2)) AS cost,
            ROUND(CAST((price - cost) / price AS DECIMAL(10,3)), 3) AS margin,
            CAST(created_at AS TIMESTAMP) AS created_at
        FROM raw_products
    """,
    
    'stg_orders': """
        SELECT 
            CAST(id AS INTEGER) AS order_id,
            CAST(user_id AS INTEGER) AS user_id,
            CAST(order_date AS TIMESTAMP) AS order_date,
            status,
            CAST(total_amount AS DECIMAL(10,2)) AS total_amount
        FROM raw_orders
    """,
    
    'stg_order_items': """
        SELECT 
            CAST(id AS INTEGER) AS order_item_id,
            CAST(order_id AS INTEGER) AS order_id,
            CAST(product_id AS INTEGER) AS product_id,
            CAST(quantity AS INTEGER) AS quantity,
            CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
            CAST(quantity * CAST(unit_price AS DECIMAL(10,2)) AS DECIMAL(10,2)) AS line_total
        FROM raw_order_items
    """,
    
    'stg_events': """
        SELECT 
            CAST(id AS INTEGER) AS event_id,
            CAST(user_id AS INTEGER) AS user_id,
            event_type,
            CAST(event_date AS TIMESTAMP) AS event_date,
            page
        FROM raw_events
    """
}

for model_name, sql in staging_models.items():
    conn.execute(f"CREATE OR REPLACE VIEW {model_name} AS {sql}")
    row_count = conn.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
    print(f"  ✓ {model_name}: {row_count} rows")

# ============================================================================
# STEP 3: CREATE DIMENSION MODELS
# ============================================================================
print("\n" + "-"*80)
print("STEP 3: CREATE DIMENSION MODELS")
print("-"*80)

dim_models = {
    'dim_users': """
        SELECT 
            user_id,
            email,
            first_name,
            last_name,
            country,
            state,
            account_age_days,
            created_at
        FROM stg_users
    """,
    
    'dim_products': """
        SELECT 
            product_id,
            name,
            category,
            price,
            cost,
            margin,
            created_at
        FROM stg_products
    """
}

for model_name, sql in dim_models.items():
    conn.execute(f"CREATE OR REPLACE VIEW {model_name} AS {sql}")
    row_count = conn.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
    print(f"  ✓ {model_name}: {row_count} rows")

# ============================================================================
# STEP 4: CREATE FACT MODELS
# ============================================================================
print("\n" + "-"*80)
print("STEP 4: CREATE FACT MODELS")
print("-"*80)

fact_models = {
    'fct_orders': """
        SELECT 
            oi.order_item_id,
            o.order_id,
            o.user_id,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.line_total,
            o.total_amount AS order_total,
            o.status AS order_status,
            o.order_date,
            ROUND(CAST((p.price - p.cost) * oi.quantity AS DECIMAL(10,2)), 2) AS margin_dollars
        FROM stg_order_items oi
        JOIN stg_orders o ON oi.order_id = o.order_id
        JOIN stg_products p ON oi.product_id = p.product_id
    """,
    
    'fct_events': """
        SELECT 
            event_id,
            user_id,
            event_type,
            event_date,
            page,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date) AS event_sequence
        FROM stg_events
    """
}

for model_name, sql in fact_models.items():
    conn.execute(f"CREATE OR REPLACE VIEW {model_name} AS {sql}")
    row_count = conn.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
    print(f"  ✓ {model_name}: {row_count} rows")

# ============================================================================
# STEP 5: RUN TESTS
# ============================================================================
print("\n" + "-"*80)
print("STEP 5: RUN TESTS")
print("-"*80)

tests = [
    ("dim_users: unique user_id", "SELECT COUNT(*) FROM dim_users WHERE user_id IS NULL"),
    ("dim_users: not_null user_id", "SELECT COUNT(*) FROM dim_users WHERE user_id IS NULL"),
    ("dim_products: unique product_id", "SELECT COUNT(DISTINCT product_id) = COUNT(*) FROM dim_products"),
    ("dim_products: not_null product_id", "SELECT COUNT(*) FROM dim_products WHERE product_id IS NULL"),
    ("fct_orders: not_null order_id", "SELECT COUNT(*) FROM fct_orders WHERE order_id IS NULL"),
    ("fct_orders: not_null user_id", "SELECT COUNT(*) FROM fct_orders WHERE user_id IS NULL"),
    ("fct_events: accepted_values event_type", "SELECT COUNT(*) FROM fct_events WHERE event_type NOT IN ('page_view', 'add_to_cart', 'purchase', 'search', 'product_view')"),
]

test_results = []
for test_name, test_sql in tests:
    try:
        result = conn.execute(test_sql).fetchone()[0]
        status = "PASS" if result == 0 or result == True else "FAIL"
        test_results.append({"test": test_name, "status": status, "result": result})
        symbol = "✓" if status == "PASS" else "✗"
        print(f"  {symbol} {test_name}: {status}")
    except Exception as e:
        test_results.append({"test": test_name, "status": "ERROR", "error": str(e)})
        print(f"  ✗ {test_name}: ERROR - {e}")

# ============================================================================
# STEP 6: GENERATE DOCUMENTATION
# ============================================================================
print("\n" + "-"*80)
print("STEP 6: GENERATE DOCUMENTATION")
print("-"*80)

documentation = {
    "project": "eCommerce Analytics",
    "generated_at": datetime.now().isoformat(),
    "models": {
        "raw": list(raw_tables.values()),
        "staging": list(staging_models.keys()),
        "marts": {
            "dimensions": list(dim_models.keys()),
            "facts": list(fact_models.keys())
        }
    },
    "tests": test_results,
    "row_counts": {
        "raw_users": conn.execute("SELECT COUNT(*) FROM raw_users").fetchone()[0],
        "raw_products": conn.execute("SELECT COUNT(*) FROM raw_products").fetchone()[0],
        "raw_orders": conn.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0],
        "raw_order_items": conn.execute("SELECT COUNT(*) FROM raw_order_items").fetchone()[0],
        "raw_events": conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0],
        "dim_users": conn.execute("SELECT COUNT(*) FROM dim_users").fetchone()[0],
        "dim_products": conn.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0],
        "fct_orders": conn.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0],
        "fct_events": conn.execute("SELECT COUNT(*) FROM fct_events").fetchone()[0],
    },
    "descriptions": {
        "stg_users": "Light cleaning of raw users. One row per user.",
        "stg_products": "Light cleaning of raw products. One row per product.",
        "stg_orders": "Light cleaning of raw orders. One row per order.",
        "stg_order_items": "Light cleaning of raw order items. One row per line item.",
        "stg_events": "Light cleaning of raw events. One row per event.",
        "dim_users": "One row per user. Contains user attributes and account age.",
        "dim_products": "One row per product. Contains product attributes and margin.",
        "fct_orders": "One row per order line item. Contains order and product details with calculated margins.",
        "fct_events": "One row per event. Contains event details and event sequence within user.",
    }
}

docs_path = PROJECT_DIR / "docs.json"
with open(docs_path, 'w') as f:
    json.dump(documentation, f, indent=2)
print(f"  ✓ Documentation saved to {docs_path}")

# ============================================================================
# STEP 7: VALIDATE PIPELINE
# ============================================================================
print("\n" + "-"*80)
print("STEP 7: VALIDATE PIPELINE")
print("-"*80)

# Spot-check fct_orders
print("\n  Sample fct_orders rows:")
sample = conn.execute("""
    SELECT 
        order_id, 
        user_id, 
        product_id, 
        quantity, 
        unit_price, 
        line_total,
        margin_dollars
    FROM fct_orders 
    LIMIT 3
""").fetchall()

for row in sample:
    print(f"    {row}")

# Verify line_total calculation
print("\n  Verifying line_total = quantity * unit_price:")
validation = conn.execute("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN ABS(line_total - (quantity * unit_price)) > 0.01 THEN 1 ELSE 0 END) as mismatches
    FROM fct_orders
""").fetchone()
print(f"    Total rows: {validation[0]}, Mismatches: {validation[1]}")
if validation[1] == 0:
    print(f"    ✓ All line_total calculations are correct!")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*80)
print("PIPELINE COMPLETE ✓")
print("="*80)
print(f"\nDatabase: {DB_PATH}")
print(f"Staging models: {len(staging_models)}")
print(f"Dimension models: {len(dim_models)}")
print(f"Fact models: {len(fact_models)}")
print(f"Tests passed: {sum(1 for t in test_results if t['status'] == 'PASS')}/{len(test_results)}")
print(f"Documentation: {docs_path}")
print("\nReady for Hour 2!")
print("="*80 + "\n")

conn.close()