
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
    CAST(EXTRACT(DAY FROM (CURRENT_TIMESTAMP - MAX(o.order_date))) AS INT) as days_since_last_order
FROM dim_users u
LEFT JOIN fct_orders o ON u.user_id = o.user_id AND o.order_status = 'completed'
GROUP BY u.user_id, u.email, u.created_at
ORDER BY lifetime_revenue DESC
LIMIT 100
