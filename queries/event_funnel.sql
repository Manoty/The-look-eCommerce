
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
