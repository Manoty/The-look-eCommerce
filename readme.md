# eCommerce Analytics Dashboard
## dbt + DuckDB + Python Analytics Pipeline

A production-ready analytics platform built in 4 hours using dbt for data transformation, DuckDB for analytics, and Python for visualization.

---

## 📋 Table of Contents
1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Project Structure](#project-structure)
4. [Data Architecture](#data-architecture)
5. [Setup Instructions](#setup-instructions)
6. [Running the Pipeline](#running-the-pipeline)
7. [Dashboard & Reports](#dashboard--reports)
8. [Key Metrics](#key-metrics)
9. [File Guide](#file-guide)
10. [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This project demonstrates a **complete analytics engineering workflow** from raw data to executive dashboards and business intelligence reports.

### What's Included:
- ✅ **Data Warehouse**: Local DuckDB database with 24K+ rows
- ✅ **Transformations**: 5 staging models, 2 dimensions, 2 fact tables
- ✅ **Quality**: 7 automated data quality tests (all passing)
- ✅ **Documentation**: Auto-generated data dictionary
- ✅ **Analytics**: 5 production-ready SQL queries
- ✅ **Dashboard**: Interactive HTML dashboard with Plotly charts
- ✅ **Reports**: Executive narrative with strategic recommendations

### Technology Stack:
| Component | Tool | Version |
|-----------|------|---------|
| Data Transformation | dbt | 1.8+ |
| Analytics Database | DuckDB | 1.4+ |
| Programming | Python | 3.11+ |
| Visualization | Plotly | 6.5+ |
| Frontend | HTML/CSS/JS | Latest |

---

## 🚀 Quick Start

### Prerequisites
```bash
# Install Python 3.11+
# (3.14 not supported due to protobuf compatibility)

# Navigate to project folder
cd C:\kev\the_look_ecommerce\ecommerce
```

### One-Command Setup
```bash
# 1. Generate raw data
python generate_csvs.py

# 2. Run full pipeline (transforms data)
python ecommerce_pipeline.py

# 3. Add metadata
python hour2_metadata.py

# 4. Generate analytics queries
python queries.py

# 5. Create interactive dashboard
python dashboard_final.py

# 6. Open dashboard
# Open dashboard.html in your browser
```

**Total time: ~2 minutes** ⏱️

---

## 📁 Project Structure

```
ecommerce/
├── raw_data/                      # Raw CSV files (source data)
│   ├── users.csv                  # 500 users
│   ├── products.csv               # 100 products
│   ├── orders.csv                 # 2,554 orders
│   ├── order_items.csv            # 7,727 line items
│   └── events.csv                 # 13,537 user events
│
├── queries/                       # SQL query files (reusable)
│   ├── revenue_by_category.sql
│   ├── top_products.sql
│   ├── user_cohort.sql
│   ├── event_funnel.sql
│   └── daily_revenue.sql
│
├── ecommerce.duckdb              # Main analytics database
│
├── Python Scripts (Orchestration)
│   ├── generate_csvs.py          # Generate synthetic raw data
│   ├── ecommerce_pipeline.py     # Load data + create models
│   ├── hour2_metadata.py         # Add documentation
│   ├── queries.py                # Run analytics queries
│   └── dashboard_final.py        # Generate HTML dashboard
│
├── Output Files
│   ├── dashboard.html            # Interactive HTML dashboard
│   ├── docs.json                 # Data dictionary
│   ├── queries.json              # Query metadata
│   └── ANALYTICS_NARRATIVE.md    # Executive report
│
└── README.md                     # This file
```

---

## 🏗️ Data Architecture

### Layer 1: RAW (Source Data)
Direct imports from CSV files. No transformations.

```
raw_users (500 rows)
raw_products (100 rows)
raw_orders (2,554 rows)
raw_order_items (7,727 rows)
raw_events (13,537 rows)
```

### Layer 2: STAGING (Data Cleaning)
Light transformations: type casting, column renaming, basic calculations.

```
stg_users           → cleaned users, account age calculated
stg_products        → cleaned products, margin calculated
stg_orders          → cleaned orders
stg_order_items     → cleaned line items, line_total calculated
stg_events          → cleaned events
```

### Layer 3: ANALYTICS (Business Ready)
Dimensions and Facts for analysis.

```
Dimensions:
  dim_users       → One row per user (500 rows)
  dim_products    → One row per product (100 rows)

Facts:
  fct_orders      → One row per order line item (7,727 rows)
  fct_events      → One row per event with sequence (13,537 rows)
```

### Data Flow Diagram:
```
raw_users ──┐
            ├──→ stg_users ──→ dim_users ──┐
raw_products├──→ stg_products→ dim_products┤
            ├──→ stg_orders ───────────────┼──→ fct_orders
raw_orders  ├──→ stg_order_items ─────────┤
            ├──→ stg_events ───────────────→ fct_events
raw_events  │
```

---

## 🔧 Setup Instructions

### Step 1: Clone/Download Project
```bash
# You already have this folder
cd C:\kev\the_look_ecommerce\ecommerce
```

### Step 2: Install Python Dependencies
```bash
# Using Python 3.11 (recommended, not 3.14)
pip install duckdb pandas plotly dbt-core dbt-duckdb
```

### Step 3: Verify Installation
```bash
dbt --version
python -c "import duckdb; print(duckdb.__version__)"
```

---

## 🏃 Running the Pipeline

### Full Pipeline (All 4 Hours)
```bash
# Hour 1: Load data + staging + marts + tests
python ecommerce_pipeline.py

# Hour 2: Add metadata
python hour2_metadata.py

# Hour 3: Generate queries
python queries.py

# Hour 4: Create dashboard
python dashboard_final.py
```

### Individual Steps
```bash
# Just generate raw data (if needed)
python generate_csvs.py

# Just run pipeline (recreate models)
python ecommerce_pipeline.py

# Just regenerate dashboard
python dashboard_final.py
```

### What Each Script Does:

| Script | Purpose | Output |
|--------|---------|--------|
| `generate_csvs.py` | Creates synthetic raw data | 5 CSV files in `raw_data/` |
| `ecommerce_pipeline.py` | Loads data, transforms, tests | `ecommerce.duckdb`, test results |
| `hour2_metadata.py` | Adds descriptions & documentation | `docs.json` updated |
| `queries.py` | Runs analytics queries | 5 SQL files + `queries.json` |
| `dashboard_final.py` | Generates interactive HTML | `dashboard.html` |

---

## 📊 Dashboard & Reports

### Interactive Dashboard
**File:** `dashboard.html`

**Features:**
- ✅ 4 key metrics (Revenue, Orders, AOV, Margin)
- ✅ Revenue by Category (bar chart)
- ✅ Top 10 Products (table)
- ✅ User Cohorts by Account Age (grouped bar)
- ✅ Event Funnel (funnel chart)
- ✅ Daily Revenue Trend (line chart)
- ✅ Key insights and findings
- ✅ Executive summary

**How to View:**
```bash
# Open in browser
dashboard.html
```

### Executive Report
**File:** `ANALYTICS_NARRATIVE.md`

**Includes:**
- Executive summary
- Revenue analysis by category
- Product performance (top 10)
- Customer cohort analysis
- Funnel conversion metrics
- Strategic recommendations
- Financial projections
- Risk & opportunities

---

## 📈 Key Metrics

### Financial Performance
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Total Revenue | $2,077,746 | $2.5M | ✓ On Track |
| Gross Margin | $1,037,859 | 52% | ⚠ At 50% |
| Avg Order Value | $778.59 | $800+ | ✓ Growing |
| Revenue Per User | $4,300 | - | ✓ Strong |

### Customer Metrics
| Metric | Value | Benchmark | Status |
|--------|-------|-----------|--------|
| Conversion Rate | 97.9% | 60-80% | ✓ Exceptional |
| Customer Retention | 96.7% | 70-85% | ✓ Exceeding |
| Active Users | 483 | - | ✓ Healthy |
| Orders Per User | 4.2 | - | ✓ Engaged |

### Category Performance (Top 3)
| Category | Revenue | % of Total | Margin |
|----------|---------|-----------|--------|
| Sports | $527,297 | 25.4% | 46% |
| Books | $424,541 | 20.4% | 55% |
| Electronics | $386,342 | 18.6% | 53% |

### Product Performance (Top 3)
| Product | Revenue | Units | Margin % |
|---------|---------|-------|----------|
| Product 54 | $48,656 | 98 | 79.6% |
| Product 74 | $42,611 | 88 | 70.8% |
| Product 95 | $41,107 | 83 | 51.6% |

---

## 📄 File Guide

### Data Files
```
raw_data/
├── users.csv          # Customer master data
├── products.csv       # Product catalog
├── orders.csv         # Order headers
├── order_items.csv    # Order line items
└── events.csv         # User events
```

### Output Files
```
ecommerce.duckdb      # SQLite-compatible database with all models
docs.json             # Data dictionary (columns, descriptions, tests)
queries.json          # Query metadata and info
dashboard.html        # Interactive web dashboard
ANALYTICS_NARRATIVE.md # Executive report
```

### Query Files
```
queries/
├── revenue_by_category.sql    # Category-level revenue analysis
├── top_products.sql           # Top 10 products by revenue
├── user_cohort.sql            # Customer segmentation by age
├── event_funnel.sql           # Conversion funnel analysis
└── daily_revenue.sql          # Daily revenue trends
```

---

## 🧪 Data Quality

### Tests Included
All 7 tests currently **PASSING** ✓

| Test | Model | Status |
|------|-------|--------|
| unique | dim_users.user_id | ✓ PASS |
| not_null | dim_users.user_id | ✓ PASS |
| unique | dim_products.product_id | ✓ PASS |
| not_null | dim_products.product_id | ✓ PASS |
| not_null | fct_orders.order_id | ✓ PASS |
| not_null | fct_orders.user_id | ✓ PASS |
| accepted_values | fct_events.event_type | ✓ PASS |

### How to Run Tests
```bash
# Tests run automatically in ecommerce_pipeline.py
python ecommerce_pipeline.py

# Look for "STEP 5: RUN TESTS" section in output
```

---

## 🔍 How to Use the Data

### Option 1: Query in DuckDB
```python
import duckdb

conn = duckdb.connect('ecommerce.duckdb')

# Revenue by category
result = conn.execute("""
    SELECT category, SUM(revenue) as total_revenue
    FROM fct_orders o
    JOIN dim_products p ON o.product_id = p.product_id
    GROUP BY category
""").fetch_all()

print(result)
```

### Option 2: Use Pre-built Queries
```bash
# Queries are in queries/ folder
# Open in your SQL IDE and run against ecommerce.duckdb
```

### Option 3: View in Dashboard
```bash
# Open dashboard.html in browser
# All visualizations are pre-built
```

---

## ⚙️ Configuration

### Change Data Sources
Edit `ecommerce_pipeline.py`, `RAW_TABLES` section:
```python
raw_tables = {
    'your_csv_name': 'table_name_in_db'
}
```

### Modify Models
Edit model SQL in `ecommerce_pipeline.py`, `staging_models` and `fact_models` sections.

### Add Tests
Edit `ecommerce_pipeline.py`, `tests` list:
```python
tests = [
    ("test_name", "SELECT COUNT(*) FROM model_name WHERE condition")
]
```

---

## 🐛 Troubleshooting

### Issue: "No such file or directory: raw_data"
**Solution:** Run `python generate_csvs.py` first

### Issue: "DuckDB not found"
**Solution:** Install with `pip install duckdb`

### Issue: Python 3.14 errors with protobuf
**Solution:** Use Python 3.11 instead
```bash
# If you have Python 3.11:
python3.11 -m pip install dbt-core dbt-duckdb
python3.11 ecommerce_pipeline.py
```

### Issue: Dashboard shows no charts
**Solution:** Make sure all queries/ files exist
```bash
# Regenerate queries
python queries.py

# Then regenerate dashboard
python dashboard_final.py
```

### Issue: Tests failing
**Solution:** Check `ecommerce.duckdb` exists
```bash
python ecommerce_pipeline.py  # Recreates database
```

---

## 📚 Learning Resources

### dbt Documentation
- [dbt Docs](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

### DuckDB
- [DuckDB Docs](https://duckdb.org/docs/)
- [SQL Reference](https://duckdb.org/docs/sql/statements/create_table)

### Analytics Engineering
- [dbt Learning](https://learn.getdbt.com/)
- [Analytics Engineering Guide](https://www.getdbt.com/what-is-analytics-engineering/)

---

## 📞 Support & Questions

### Common Tasks

**Q: How do I add a new metric?**
A: Add a SQL query to `queries/` folder, then update `queries.py` to run it and include in dashboard.

**Q: Can I connect to different data sources?**
A: Yes! DuckDB supports CSV, JSON, Parquet, PostgreSQL, and more. Edit `ecommerce_pipeline.py` to change source.

**Q: How do I schedule this to run daily?**
A: Use Windows Task Scheduler or Airflow to run `ecommerce_pipeline.py` on a schedule.

**Q: Can I use this with real data?**
A: Yes! Replace `raw_data/` CSV files with your actual data, keep same column names, run pipeline.

---

## 📝 Project Summary

**Total Data Points:** 24,370 rows  
**Models Created:** 9 (5 staging + 2 dimensions + 2 facts)  
**Tests Passing:** 7/7 (100%)  
**Queries Created:** 5 (production-ready)  
**Documentation:** Auto-generated  

### What This Demonstrates:
✅ End-to-end analytics pipeline  
✅ Data quality testing  
✅ Production-grade SQL  
✅ Executive reporting  
✅ Interactive dashboards  
✅ Scalable architecture  

---

## 🎯 Next Steps

1. **Customize for your data** - Replace raw_data/ files with real data
2. **Add more metrics** - Create additional SQL queries in queries/ folder
3. **Schedule runs** - Set up daily/weekly pipeline execution
4. **Share dashboards** - Host dashboard.html on web server
5. **Expand scope** - Add more sources, models, and reports

---

## 📄 License & Attribution

**Built with:**
- dbt (Apache 2.0)
- DuckDB (MIT)
- Plotly (MIT)

**Data:** Synthetic eCommerce dataset (educational purposes)

---

## 👤 Author

Created by Kev as part of analytics engineering demonstration.  
Last Updated: February 7, 2026

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-07 | Initial release  |

---

**Questions?** Check the troubleshooting section or review the ANALYTICS_NARRATIVE.md for detailed insights.

**Ready to analyze your own data?** Follow the setup instructions and you'll be running analytics queries within minutes!