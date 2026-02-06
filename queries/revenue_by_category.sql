
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
