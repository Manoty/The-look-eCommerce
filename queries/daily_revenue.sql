
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
