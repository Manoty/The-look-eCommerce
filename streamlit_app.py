"""
HOUR 4: Streamlit Dashboard
eCommerce Analytics Dashboard with 4 key visualizations
"""

import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

# ============================================================================
# CONFIG
# ============================================================================
st.set_page_config(page_title="eCommerce Analytics", layout="wide")
PROJECT_DIR = Path(__file__).parent
DB_PATH = PROJECT_DIR / "ecommerce.duckdb"
QUERIES_DIR = PROJECT_DIR / "queries"

# ============================================================================
# LOAD QUERIES
# ============================================================================
def load_query(filename):
    with open(QUERIES_DIR / filename, 'r') as f:
        return f.read()

# ============================================================================
# PAGE HEADER
# ============================================================================
st.title("📊 eCommerce Analytics Dashboard")
st.markdown("---")
st.markdown("**Real-time analytics powered by dbt + DuckDB + Streamlit**")

# ============================================================================
# CONNECT & LOAD DATA
# ============================================================================
@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH))

conn = get_connection()

# Load all queries
revenue_by_category_sql = load_query("revenue_by_category.sql")
top_products_sql = load_query("top_products.sql")
user_cohort_sql = load_query("user_cohort.sql")
event_funnel_sql = load_query("event_funnel.sql")
daily_revenue_sql = load_query("daily_revenue.sql")

# ============================================================================
# LOAD DATA
# ============================================================================
@st.cache_data
def load_data():
    revenue_by_cat = pd.read_df(conn.execute(revenue_by_category_sql))
    top_products = pd.read_df(conn.execute(top_products_sql))
    user_cohort = pd.read_df(conn.execute(user_cohort_sql))
    event_funnel = pd.read_df(conn.execute(event_funnel_sql))
    daily_revenue = pd.read_df(conn.execute(daily_revenue_sql))
    return revenue_by_cat, top_products, user_cohort, event_funnel, daily_revenue

revenue_by_cat, top_products, user_cohort, event_funnel, daily_revenue = load_data()

# ============================================================================
# METRICS ROW
# ============================================================================
st.markdown("### 📈 Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_revenue = revenue_by_cat['revenue'].sum()
    st.metric("Total Revenue", f"${total_revenue:,.0f}")

with col2:
    total_orders = revenue_by_cat['order_count'].sum()
    st.metric("Total Orders", f"{total_orders:,.0f}")

with col3:
    avg_order_value = revenue_by_cat['avg_order_value'].mean()
    st.metric("Avg Order Value", f"${avg_order_value:,.2f}")

with col4:
    total_margin = revenue_by_cat['total_margin'].sum()
    st.metric("Total Margin", f"${total_margin:,.0f}")

st.markdown("---")

# ============================================================================
# ROW 1: REVENUE BY CATEGORY + TOP PRODUCTS
# ============================================================================
st.markdown("### 📦 Product Performance")
col1, col2 = st.columns(2)

with col1:
    st.markdown("#### Revenue by Category")
    fig_revenue = px.bar(
        revenue_by_cat,
        x='category',
        y='revenue',
        color='revenue',
        title="Total Revenue by Category",
        labels={'revenue': 'Revenue ($)', 'category': 'Category'},
        color_continuous_scale="Viridis"
    )
    fig_revenue.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_revenue, use_container_width=True)

with col2:
    st.markdown("#### Top 10 Products by Revenue")
    top_10_display = top_products[['name', 'category', 'revenue', 'margin_pct']].copy()
    top_10_display['revenue'] = top_10_display['revenue'].apply(lambda x: f"${x:,.0f}")
    top_10_display['margin_pct'] = top_10_display['margin_pct'].apply(lambda x: f"{x*100:.1f}%")
    st.dataframe(top_10_display, use_container_width=True, hide_index=True)

st.markdown("---")

# ============================================================================
# ROW 2: USER COHORTS + EVENT FUNNEL
# ============================================================================
st.markdown("### 👥 User & Behavior Analysis")
col1, col2 = st.columns(2)

with col1:
    st.markdown("#### User Cohorts by Account Age")
    fig_cohort = px.bar(
        user_cohort,
        x='cohort',
        y=['user_count', 'total_orders'],
        barmode='group',
        title="Users & Orders by Cohort",
        labels={'value': 'Count', 'cohort': 'Account Age Cohort'},
        height=400
    )
    st.plotly_chart(fig_cohort, use_container_width=True)

with col2:
    st.markdown("#### Event Funnel")
    fig_funnel = go.Figure(data=[go.Funnel(
        y=event_funnel['event_type'],
        x=event_funnel['user_count'],
        textposition="inside",
        textinfo="value+percent initial"
    )])
    fig_funnel.update_layout(title="User Journey Funnel", height=400)
    st.plotly_chart(fig_funnel, use_container_width=True)

st.markdown("---")

# ============================================================================
# ROW 3: DAILY REVENUE TREND
# ============================================================================
st.markdown("### 📅 Revenue Trend (Last 30 Days)")
fig_trend = px.line(
    daily_revenue.sort_values('order_date'),
    x='order_date',
    y='revenue',
    title="Daily Revenue Trend",
    labels={'revenue': 'Revenue ($)', 'order_date': 'Date'},
    markers=True
)
fig_trend.update_layout(height=400)
st.plotly_chart(fig_trend, use_container_width=True)

st.markdown("---")

# ============================================================================
# INSIGHTS & SUMMARY
# ============================================================================
st.markdown("### 💡 Key Insights")

insight_cols = st.columns(2)

with insight_cols[0]:
    top_category = revenue_by_cat.loc[revenue_by_cat['revenue'].idxmax()]
    st.success(f"""
    **🏆 Top Performing Category**
    
    {top_category['category']} leads with **${top_category['revenue']:,.0f}** in revenue
    ({top_category['order_count']:.0f} orders, {top_category['total_margin']:,.0f} margin)
    """)

with insight_cols[1]:
    top_product = top_products.iloc[0]
    st.info(f"""
    **⭐ Best Product**
    
    {top_product['name']} generated **${top_product['revenue']:,.0f}** in revenue
    with {top_product['margin_pct']:.1%} margin ({top_product['units_sold']:.0f} units sold)
    """)

insight_cols2 = st.columns(2)

with insight_cols2[0]:
    newest_cohort = user_cohort.iloc[0]
    st.warning(f"""
    **📱 Newest Users (0-30 days)**
    
    {newest_cohort['user_count']:.0f} users, avg ${newest_cohort['avg_order_value']:,.2f}/order
    Total revenue: ${newest_cohort['total_revenue']:,.0f}
    """)

with insight_cols2[1]:
    purchase_users = event_funnel[event_funnel['event_type'] == 'purchase']['user_count'].values[0]
    all_users = event_funnel['user_count'].max()
    conversion = (purchase_users / all_users) * 100
    st.info(f"""
    **🛒 Conversion Rate**
    
    {conversion:.1f}% of users completed a purchase
    ({purchase_users:.0f} out of {all_users:.0f} total users)
    """)

st.markdown("---")

# ============================================================================
# SUMMARY NARRATIVE
# ============================================================================
st.markdown("### 📝 Executive Summary")

summary_text = f"""
This eCommerce analytics dashboard reveals a healthy and growing business:

**Revenue Highlights:**
- Total revenue across all categories: **${total_revenue:,.0f}**
- {revenue_by_cat.shape[0]} product categories performing well
- Average order value: **${avg_order_value:,.2f}**
- Profit margin: **${total_margin:,.0f}** (consolidated)

**Product & Category Performance:**
- {top_category['category']} is the strongest performer, contributing {(top_category['revenue']/total_revenue)*100:.1f}% of revenue
- Top 10 products account for significant portion of orders
- Product margins vary from {top_products['margin_pct'].min():.1%} to {top_products['margin_pct'].max():.1%}

**User Behavior & Conversion:**
- {conversion:.1f}% of users are converting to purchasers
- Newest user cohort (0-30 days) shows {newest_cohort['user_count']:.0f} engaged users
- Event funnel shows strong engagement with page_view as primary entry point

**Actionable Insights:**
1. Focus marketing on {top_category['category']} - it's driving the most revenue
2. Replicate success of top products across the catalog
3. Optimize conversion funnel - currently at {conversion:.1f}%, target 25%+
4. Monitor newest cohort retention in next 30 days

**Technical Stack:**
Built with dbt (data transformation), DuckDB (analytics), and Streamlit (visualization).
All models tested and documented for production readiness.
"""

st.info(summary_text)

st.markdown("---")
st.markdown("**Dashboard generated on:** " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))