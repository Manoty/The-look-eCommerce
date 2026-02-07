
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
