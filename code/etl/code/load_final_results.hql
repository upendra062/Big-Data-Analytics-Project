use video_analytics;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE top_channels
PARTITION(batchid)
SELECT 
channel_id, 
COUNT(DISTINCT video_id) AS total_distinct_videos_played, 
COUNT(DISTINCT user_id) AS distinct_user_count,
batchid
FROM enriched_data
WHERE status='pass'
AND liked="True"
GROUP BY channel_id, batchId
ORDER BY total_distinct_videos_played DESC
LIMIT 10;


INSERT OVERWRITE TABLE users_behaviour
PARTITION(batchid)
SELECT 
CASE WHEN (su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED'
WHEN (su.user_id IS NOT NULL AND CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED'
END AS user_type,
SUM(minutes_played) AS duration,
batchid
FROM enriched_data ed
LEFT OUTER JOIN subscribed_users su
ON ed.user_id=su.user_id
WHERE ed.status='pass'
GROUP BY CASE WHEN (su.user_id IS NULL OR CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED'
WHEN (su.user_id IS NOT NULL AND CAST(ed.timestamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END, batchid;


INSERT OVERWRITE TABLE connected_creators
PARTITION(batchid)
SELECT 
ua.creator_id, 
COUNT(DISTINCT ua.user_id) AS user_count,
batchId
FROM
(
SELECT user_id, creator_id FROM users_creator
LATERAL VIEW explode(creators_array) creators AS creator_id
) ua
INNER JOIN
(
SELECT creator_id, video_id, user_id,batchid
FROM enriched_data
WHERE status='pass'
) ed
ON ua.creator_id=ed.creator_id
AND ua.user_id=ed.user_id
GROUP BY ua.creator_id,ed.batchId
ORDER BY user_count DESC
LIMIT 10;


INSERT OVERWRITE TABLE top_royalty_videos
PARTITION(batchid)
SELECT video_id,
SUM(minutes_played) AS duration,
batchId
FROM enriched_data
WHERE status='pass'
AND (liked="True" OR video_end_type=0)
GROUP BY video_id,batchId
ORDER BY duration DESC
LIMIT 10;



INSERT OVERWRITE TABLE top_unsubscribed_users
PARTITION(batchid)
SELECT 
ed.user_id,
SUM(ed.minutes_played) AS duration,
ed.batchid
FROM enriched_data ed
LEFT OUTER JOIN subscribed_users su
ON ed.user_id=su.user_id
WHERE ed.status='pass'
AND (su.user_id IS NULL OR (CAST(ed.timestamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))))
GROUP BY ed.user_id,ed.batchid
ORDER BY duration DESC
LIMIT 10;
