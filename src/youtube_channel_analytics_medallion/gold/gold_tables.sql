CREATE TABLE IF NOT EXISTS gold.yt.youtube_num_statistics_obt(
    date_of_data DATE, 
    run_id VARCHAR(100),
    hk_channel VARCHAR(100), 
    hk_video VARCHAR(100), 
    hk_comment VARCHAR(100),
    channel_view_count INT, 
    channel_subscrb_count INT, 
    channel_hidden_subscrb_count INT, 
    video_count INT, 
    video_view_count INT, 
    video_like_count INT, 
    video_favorite_count INT, 
    video_comment_count INT, 
    comment_like_count INT, 
    comment_reply_count INT
)
USING DELTA
PARTITIONED BY (date_of_data);


CREATE TABLE IF NOT EXISTS gold.yt.youtube_statistics_obt(
    valid_from DATE, 
    valid_to DATE, 
    run_id VARCHAR(100),
    hk_channel VARCHAR(100), 
    hk_video VARCHAR(100), 
    hk_comment VARCHAR(100),
    channel_name VARCHAR(50), 
    custom_url VARCHAR(200), 
    description VARCHAR(2000),
    channel_publish_date TIMESTAMP, 
    video_publish_date TIMESTAMP, 
    video_title VARCHAR(100), 
    video_description VARCHAR(2000),
    video_category_id INT, 
    video_duration VARCHAR(20), 
    video_caption BOOLEAN, 
    video_dflt_language VARCHAR(4),
    video_dflt_audio_language VARCHAR(4), 
    comment_text VARCHAR(2000), 
    comment_author VARCHAR(50), 
    comment_publish_date TIMESTAMP, 
    comment_update_date TIMESTAMP, 
    is_public BOOLEAN,
    can_reply BOOLEAN, 
    is_current BOOLEAN, 
    hash_diff VARCHAR(70)
)
USING DELTA