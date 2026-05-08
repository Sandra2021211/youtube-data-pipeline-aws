-- This query retrieves the count of videos for each region from the raw_statistics table.

select region, count(*) as video_count
from raw_statistics
group by region
order by video_count desc;

-- This query retrieves the top 10 videos in the 'in' region based on views, along with their trending date, title, and likes.

select video_id, trending_date, title, views, likes
from raw_statistics
where region = 'in'
limit 10;