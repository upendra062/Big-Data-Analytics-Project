set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE video_analytics.enriched_data
	SELECT 
	IF (i.liked IS NULL, 0, i.liked) AS liked,i.user_id ,IF (i.video_end_type IS NULL, 3, i.video_end_type) AS video_end_type ,i.minutes_played,i.video_id,sg.geo_cd ,i.channel_id ,sa.creator_id  ,i.timestamp  ,i.disliked,
	IF((i.liked="true" AND i.disliked="true") 
	OR i.user_id IS NULL 
	OR i.video_id IS NULL
	OR i.timestamp IS NULL
	OR i.geo_cd IS NULL
	OR i.user_id='' 
	OR i.video_id='' 
	OR i.timestamp='' 
	OR i.geo_cd=''
	OR sg.geo_cd IS NULL
	OR sg.geo_cd=''
	OR sa.creator_id IS NULL
	OR sa.creator_id='', 'fail', 'pass') AS status
	FROM video_analytics.Merged_data_all i LEFT OUTER JOIN video_analytics.channel_geo_map sg ON i.channel_id = sg.channel_id
	LEFT OUTER JOIN video_analytics.video_creator_map sa ON i.video_id = sa.video_id;
