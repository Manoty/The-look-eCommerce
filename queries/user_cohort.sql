
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
