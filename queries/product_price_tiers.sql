
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
