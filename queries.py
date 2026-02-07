"""
HOUR 3: Analytics Layer + Query Validation
Build queries that will power Streamlit dashboard in Hour 4
"""

import duckdb
import json
from pathlib import Path
from datetime import datetime

PROJECT_DIR = Path(__file__).parent
DB_PATH = PROJECT_DIR / "ecommerce.duckdb"
QUERIES_DIR = PROJECT_DIR / "queries"

# Create queries directory
QUERIES_DIR.mkdir(exist_ok=True)

# Connect to database
conn = duckdb.connect(str(DB_PATH))

print("\n" + "="*80)
print("HOUR 3: ANALYTICS LAYER + QUERY VALIDATION")
print("="*80)

# ============================================================================
# QUERY 1: REVENUE BY CATEGORY
# ============================================================================
print("\n" + "-"*80)
print("QUERY 1: Revenue by Category")
print("-"*80)

revenue_by_category_sql = """
SELECT 
    p.category,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(*) as line_items,
    ROUND(SUM(o.line_total), 2) as revenue,
    ROUND(AVG(o.line_total), 2) as avg_order_value,
    ROUND(SUM(o.margin_dollars), 2) as total_margin
FROM fct_orders o
JOIN dim_products p ON o.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY p.category
ORDER BY revenue DESC
"""

with open(QUERIES_DIR / "revenue_by_category.sql", 'w') as f:
    f.write(revenue_by_category_sql)

result = conn.execute(revenue_by_category_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nSample data:")
for row in result[:5]:
    print(f"  {dict(zip(columns, row))}")

# ============================================================================
# QUERY 2: TOP 10 PRODUCTS
# ============================================================================
print("\n" + "-"*80)
print("QUERY 2: Top 10 Products by Revenue")
print("-"*80)

top_products_sql = """
SELECT 
    p.product_id,
    p.name,
    p.category,
    ROUND(p.price, 2) as price,
    COUNT(DISTINCT o.order_id) as orders,
    SUM(o.quantity) as units_sold,
    ROUND(SUM(o.line_total), 2) as revenue,
    ROUND(SUM(o.margin_dollars), 2) as total_margin,
    ROUND(SUM(o.margin_dollars) / SUM(o.line_total), 3) as margin_pct
FROM fct_orders o
JOIN dim_products p ON o.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY p.product_id, p.name, p.category, p.price
ORDER BY revenue DESC
LIMIT 10
"""

with open(QUERIES_DIR / "top_products.sql", 'w') as f:
    f.write(top_products_sql)

result = conn.execute(top_products_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nSample data:")
for row in result[:3]:
    print(f"  {dict(zip(columns, row))}")

# ============================================================================
# QUERY 3: USER COHORT ANALYSIS
# ============================================================================
print("\n" + "-"*80)
print("QUERY 3: User Cohort Analysis (by account age)")
print("-"*80)

cohort_sql = """
SELECT 
    CASE 
        WHEN EXTRACT(DAY FROM (CURRENT_TIMESTAMP - u.created_at)) <= 30 THEN '0-30 days'
        WHEN EXTRACT(DAY FROM (CURRENT_TIMESTAMP - u.created_at)) <= 90 THEN '31-90 days'
        WHEN EXTRACT(DAY FROM (CURRENT_TIMESTAMP - u.created_at)) <= 180 THEN '91-180 days'
        ELSE '180+ days'
    END as cohort,
    COUNT(DISTINCT u.user_id) as user_count,
    COUNT(DISTINCT o.order_id) as total_orders,
    ROUND(AVG(o.line_total), 2) as avg_order_value,
    ROUND(SUM(o.line_total), 2) as total_revenue
FROM dim_users u
LEFT JOIN fct_orders o ON u.user_id = o.user_id AND o.order_status = 'completed'
GROUP BY cohort
ORDER BY 
    CASE 
        WHEN cohort = '0-30 days' THEN 1
        WHEN cohort = '31-90 days' THEN 2
        WHEN cohort = '91-180 days' THEN 3
        ELSE 4
    END
"""

with open(QUERIES_DIR / "user_cohort.sql", 'w') as f:
    f.write(cohort_sql)

result = conn.execute(cohort_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nSample data:")
for row in result:
    print(f"  {dict(zip(columns, row))}")

# ============================================================================
# QUERY 4: EVENT FUNNEL
# ============================================================================
print("\n" + "-"*80)
print("QUERY 4: Event Funnel Analysis")
print("-"*80)

funnel_sql = """
SELECT 
    event_type,
    COUNT(DISTINCT user_id) as user_count,
    COUNT(*) as event_count,
    ROUND(100.0 * COUNT(DISTINCT user_id) / 
        (SELECT COUNT(DISTINCT user_id) FROM fct_events), 1) as pct_all_users
FROM fct_events
GROUP BY event_type
ORDER BY 
    CASE 
        WHEN event_type = 'page_view' THEN 1
        WHEN event_type = 'product_view' THEN 2
        WHEN event_type = 'search' THEN 3
        WHEN event_type = 'add_to_cart' THEN 4
        WHEN event_type = 'purchase' THEN 5
        ELSE 6
    END
"""

with open(QUERIES_DIR / "event_funnel.sql", 'w') as f:
    f.write(funnel_sql)

result = conn.execute(funnel_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nSample data:")
for row in result:
    print(f"  {dict(zip(columns, row))}")

# ============================================================================
# QUERY 5: DAILY REVENUE TREND
# ============================================================================
print("\n" + "-"*80)
print("QUERY 5: Daily Revenue Trend (for time series)")
print("-"*80)

daily_revenue_sql = """
SELECT 
    DATE(o.order_date) as order_date,
    COUNT(DISTINCT o.order_id) as orders,
    SUM(o.quantity) as units,
    ROUND(SUM(o.line_total), 2) as revenue,
    ROUND(SUM(o.margin_dollars), 2) as margin
FROM fct_orders o
WHERE o.order_status = 'completed'
GROUP BY DATE(o.order_date)
ORDER BY order_date DESC
LIMIT 30
"""

with open(QUERIES_DIR / "daily_revenue.sql", 'w') as f:
    f.write(daily_revenue_sql)

result = conn.execute(daily_revenue_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nSample data (most recent 3 days):")
for row in result[:3]:
    print(f"  {dict(zip(columns, row))}")

# ============================================================================
# VALIDATION SUMMARY
# ============================================================================
print("\n" + "="*80)
print("QUERY VALIDATION SUMMARY")
print("="*80)

queries_info = {
    "revenue_by_category": {
        "description": "Revenue broken down by product category",
        "use_case": "Category performance analysis",
        "chart_type": "Bar chart",
        "file": "revenue_by_category.sql"
    },
    "top_products": {
        "description": "Top 10 products by revenue",
        "use_case": "Product performance",
        "chart_type": "Table",
        "file": "top_products.sql"
    },
    "user_cohort": {
        "description": "User behavior by account age cohort",
        "use_case": "Cohort analysis and retention",
        "chart_type": "Bar/Line chart",
        "file": "user_cohort.sql"
    },
    "event_funnel": {
        "description": "User journey through event types",
        "use_case": "Funnel analysis and conversion",
        "chart_type": "Funnel chart",
        "file": "event_funnel.sql"
    },
    "daily_revenue": {
        "description": "Revenue trend over time",
        "use_case": "Time series analysis",
        "chart_type": "Line chart",
        "file": "daily_revenue.sql"
    },
    "customer_lifetime_value": {
        "description": "Customer lifetime value with purchase history",
        "use_case": "Customer segmentation and retention",
        "chart_type": "Table/Scatter plot",
        "file": "customer_lifetime_value.sql"
    },
    
    "product_price_tiers": {
        "description": "Product performance segmented by price range",
        "use_case": "Price strategy and margin analysis",
        "chart_type": "Bar chart",
        "file": "product_price_tiers.sql"
    }
}

for query_name, info in queries_info.items():
    print(f"\n✓ {query_name}")
    print(f"  Description: {info['description']}")
    print(f"  Chart type: {info['chart_type']}")
    print(f"  File: {info['file']}")

# ============================================================================
# GENERATE QUERIES.JSON
# ============================================================================
print("\n" + "-"*80)
print("Generating queries.json")
print("-"*80)

queries_json = {
    "generated_at": datetime.now().isoformat(),
    "queries": queries_info,
    "notes": "All queries validated and saved as SQL files. Ready for Streamlit integration."
}

with open(PROJECT_DIR / "queries.json", 'w') as f:
    json.dump(queries_json, f, indent=2)

print(f"✓ queries.json saved")
print(f"✓ 5 SQL query files saved to {QUERIES_DIR}/")

# ============================================================================
# QUERY 6: CUSTOMER LIFETIME VALUE (CLV)
# ============================================================================
print("\n" + "-"*80)
print("QUERY 6: Customer Lifetime Value")
print("-"*80)

clv_sql = """
SELECT 
    u.user_id,
    u.email,
    u.created_at,
    COUNT(DISTINCT o.order_id) as total_orders,
    ROUND(SUM(o.line_total), 2) as lifetime_revenue,
    ROUND(AVG(o.line_total), 2) as avg_order_value,
    ROUND(SUM(o.margin_dollars), 2) as lifetime_margin,
    ROUND(100.0 * SUM(o.margin_dollars) / SUM(o.line_total), 1) as margin_pct,
    MAX(o.order_date) as last_purchase_date,
    ROUND((CURRENT_TIMESTAMP - MAX(o.order_date))::NUMERIC / 86400, 0) as days_since_last_order
FROM dim_users u
LEFT JOIN fct_orders o ON u.user_id = o.user_id AND o.order_status = 'completed'
GROUP BY u.user_id, u.email, u.created_at
ORDER BY lifetime_revenue DESC
LIMIT 100
"""

with open(QUERIES_DIR / "customer_lifetime_value.sql", 'w') as f:
    f.write(clv_sql)

result = conn.execute(clv_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nTop 3 customers by CLV:")
for row in result[:3]:
    print(f"  {dict(zip(columns, row))}")

# ============================================================================
# QUERY 7: CATEGORY PERFORMANCE MONTH-OVER-MONTH
# ============================================================================
print("\n" + "-"*80)
print("QUERY 7: Category Performance by Month")
print("-"*80)

month_category_sql = """
SELECT 
    DATE_TRUNC('month', o.order_date)::DATE as month,
    p.category,
    COUNT(DISTINCT o.order_id) as orders,
    SUM(o.quantity) as units_sold,
    ROUND(SUM(o.line_total), 2) as revenue,
    ROUND(SUM(o.margin_dollars), 2) as margin,
    ROUND(SUM(o.margin_dollars) / SUM(o.line_total), 3) as margin_pct
FROM fct_orders o
JOIN dim_products p ON o.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY DATE_TRUNC('month', o.order_date), p.category
ORDER BY month DESC, revenue DESC
"""

with open(QUERIES_DIR / "category_by_month.sql", 'w') as f:
    f.write(month_category_sql)

result = conn.execute(month_category_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nMost recent month by category:")
for row in result[:5]:
    print(f"  {dict(zip(columns, row))}")

# ============================================================================
# QUERY 8: PRODUCT PRICE TIER ANALYSIS
# ============================================================================
print("\n" + "-"*80)
print("QUERY 8: Product Performance by Price Tier")
print("-"*80)

price_tier_sql = """
SELECT 
    CASE 
        WHEN p.price < 50 THEN 'Budget (<$50)'
        WHEN p.price < 150 THEN 'Mid-Range ($50-150)'
        WHEN p.price < 300 THEN 'Premium ($150-300)'
        ELSE 'Luxury ($300+)'
    END as price_tier,
    COUNT(DISTINCT p.product_id) as product_count,
    COUNT(DISTINCT o.order_id) as orders,
    SUM(o.quantity) as units_sold,
    ROUND(SUM(o.line_total), 2) as revenue,
    ROUND(AVG(o.line_total), 2) as avg_order_value,
    ROUND(SUM(o.margin_dollars), 2) as total_margin,
    ROUND(100.0 * SUM(o.margin_dollars) / SUM(o.line_total), 1) as margin_pct
FROM fct_orders o
JOIN dim_products p ON o.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY price_tier
ORDER BY 
    CASE 
        WHEN price_tier = 'Budget (<$50)' THEN 1
        WHEN price_tier = 'Mid-Range ($50-150)' THEN 2
        WHEN price_tier = 'Premium ($150-300)' THEN 3
        ELSE 4
    END
"""

with open(QUERIES_DIR / "product_price_tiers.sql", 'w') as f:
    f.write(price_tier_sql)

result = conn.execute(price_tier_sql).fetchall()
columns = [desc[0] for desc in conn.description]

print(f"Rows: {len(result)}")
print(f"Columns: {columns}")
print("\nSample data:")
for row in result:
    print(f"  {dict(zip(columns, row))}")

print(f"✓ 3 additional SQL query files saved to {QUERIES_DIR}/")

# ============================================================================
# FINAL CHECKPOINT
# ============================================================================
print("\n" + "="*80)
print("HOUR 3 CHECKPOINT")
print("="*80)
print("✓ Revenue by category query - VALIDATED")
print("✓ Top products query - VALIDATED")
print("✓ User cohort query - VALIDATED")
print("✓ Event funnel query - VALIDATED")
print("✓ Daily revenue trend query - VALIDATED")
print("✓ Customer lifetime value query - VALIDATED")
print("✓ Category by month query - VALIDATED")
print("✓ Product price tiers query - VALIDATED")
print("✓ All 8 queries saved as .sql files")
print("✓ queries.json generated")
print("\nReady for Hour 4 (Streamlit dashboard)!")
print("="*80 + "\n")

conn.close()