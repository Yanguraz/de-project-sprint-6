-- DDL
DROP TABLE IF EXISTS STV2024031225__STAGING.group_log;

CREATE TABLE STV2024031225__STAGING.group_log (
	group_id int,
	user_id int,
	user_id_from int,
	event varchar(6),
	date_time datetime
);

DROP TABLE IF EXISTS STV2024031225__STAGING.l_user_group_activity;

CREATE TABLE STV2024031225__DWH.l_user_group_activity (
	hk_l_user_group_activity int PRIMARY KEY,
	hk_user_id int NOT NULL REFERENCES STV2024031225__DWH.h_users (hk_user_id),
	hk_group_id int NOT NULL REFERENCES STV2024031225__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
);

DROP TABLE IF EXISTS STV2024031225__STAGING.s_auth_history;

CREATE TABLE STV2024031225__DWH.s_auth_history (
	hk_l_user_group_activity int NOT NULL REFERENCES STV2024031225__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(6),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
);

-- Insert

INSERT INTO STV2024031225__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
	hash(gl.group_id, gl.user_id) AS hk_l_user_group_activity,
	hu.hk_user_id,
	hg.hk_group_id,
	now() AS load_dt,
	'S3' AS load_src
FROM STV2024031225__STAGING.group_log AS gl
LEFT JOIN STV2024031225__DWH.h_users AS hu ON gl.user_id = hu.user_id 
LEFT JOIN STV2024031225__DWH.h_groups AS hg ON gl.group_id = hg.group_id 
;

INSERT INTO STV2024031225__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT
	uga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.date_time AS event_dt,
	now() AS load_dt,
	'S3' AS load_src
FROM STV2024031225__STAGING.group_log AS gl
LEFT JOIN STV2024031225__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN STV2024031225__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN STV2024031225__DWH.l_user_group_activity AS uga ON hg.hk_group_id = uga.hk_group_id AND hu.hk_user_id = uga.hk_user_id
; 


--Mart
WITH user_group_log AS (
    SELECT
        hg.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users,
        MAX(hg.registration_dt) AS max_registration_dt
    FROM STV2024031225__DWH.h_groups hg 
    LEFT JOIN STV2024031225__DWH.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id 
    INNER JOIN STV2024031225__DWH.s_auth_history sah ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity 
    WHERE sah.event = 'add'
    GROUP BY hg.hk_group_id
),
user_group_messages AS (
    SELECT 
        hg.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV2024031225__DWH.h_groups hg 
    LEFT JOIN STV2024031225__DWH.l_groups_dialogs lgd ON hg.hk_group_id = lgd.hk_group_id 
    INNER JOIN STV2024031225__DWH.l_user_message lum ON lgd.hk_message_id = lum.hk_message_id 
    GROUP BY hg.hk_group_id 
)
SELECT  
    ugl.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY ugl.cnt_added_users DESC
LIMIT 10;

