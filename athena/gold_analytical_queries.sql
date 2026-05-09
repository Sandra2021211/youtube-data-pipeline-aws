--  Top categories by views

SELECT
    category_name,
    total_views
FROM category_analytics
ORDER BY total_views DESC
LIMIT 10;

-- Which regions trend the most

SELECT
    region,
    COUNT(*) AS trending_videos
FROM trending_analytics
GROUP BY region
ORDER BY trending_videos DESC;

-- Row counts for all tables

SELECT 'category_analytics' AS table_name, COUNT(*) FROM category_analytics
UNION ALL
SELECT 'channel_analytics', COUNT(*) FROM channel_analytics
UNION ALL
SELECT 'trending_analytics', COUNT(*) FROM trending_analytics;