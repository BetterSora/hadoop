--日活指标统计
--各维度组合分析：
不区分操作系统os_name 不区分城市city 不区分渠道release_channel 不区分版本app_ver_name 活跃用户 
区分操作系统os_name 不区分城市city 不区分渠道release_channel 不区分版本app_ver_name 活跃用户 
不区分操作系统os_name 区分城市city 不区分渠道release_channel 不区分版本app_ver_name 活跃用户 
不区分操作系统os_name 不区分城市city 区分渠道release_channel 不区分版本app_ver_name 活跃用户 
不区分操作系统os_name 不区分城市city 不区分渠道release_channel 区分版本app_ver_name 活跃用户 
区分操作系统os_name 区分城市city 不区分渠道release_channel 不区分版本app_ver_name 活跃用户
.....

维度组合统计 
0 0 0 0 
0 0 0 1 
0 0 1 0 
0 0 1 1 
0 1 0 0 
0 1 0 1 
0 1 1 0 
0 1 1 1 
1 0 0 0 
1 0 0 1 
1 0 1 0 
1 0 1 1 
1 1 0 0 
1 1 0 1 
1 1 1 0 
1 1 1 1

-- 1/ 把当天的活跃用户信息抽取出来，存入一个日活用户信息表
-- 1.1/ 建日活用户信息表
CREATE TABLE app.etl_user_active_day (
    sdk_ver string
    ,time_zone string
    ,commit_id string
    ,commit_time string
    ,pid string
    ,app_token string
    ,app_id string
    ,device_id string
    ,device_id_type string
    ,release_channel string
    ,app_ver_name string
    ,app_ver_code string
    ,os_name string
    ,os_ver string
    ,language string
    ,country string
    ,manufacture string
    ,device_model string
    ,resolution string
    ,net_type string
    ,account string
    ,app_device_id string
    ,mac string
    ,android_id string
    ,imei string
    ,cid_sn string
    ,build_num string
    ,mobile_data_type string
    ,promotion_channel string
    ,carrier string
    ,city string
    ,user_id string
    ) partitioned BY (day string) row format delimited fields terminated BY '\001';

	-- 1.2 从ods_app_log原始数据表的当天分区中，抽取当日的日活用户信息插入日活用户信息表etl_user_active_day
	-- 注意点：每个活跃用户抽取他当天所有记录中时间最早的一条；
INSERT INTO TABLE app.etl_user_active_day PARTITION (day = '2017-08-14')
SELECT sdk_ver
    ,time_zone
    ,commit_id
    ,commit_time
    ,pid
    ,app_token
    ,app_id
    ,device_id
    ,device_id_type
    ,release_channel
    ,app_ver_name
    ,app_ver_code
    ,os_name
    ,os_ver
    ,LANGUAGE
    ,country
    ,manufacture
    ,device_model
    ,resolution
    ,net_type
    ,account
    ,app_device_id
    ,mac
    ,android_id
    ,imei
    ,cid_sn
    ,build_num
    ,mobile_data_type
    ,promotion_channel
    ,carrier
    ,city
    ,user_id
FROM (
    SELECT *
        ,row_number() OVER (
            PARTITION BY user_id ORDER BY commit_time
            ) AS rn
    FROM app.ods_app_log
    WHERE day = '2017-08-14'
    ) tmp
WHERE rn = 1;



--维度统计
--建维度统计结果表 dim_user_active_day
DROP TABLE app.dim_user_active_day;
CREATE TABLE app.dim_user_active_day (
    os_name string
    ,city string
    ,release_channel string
    ,app_ver_name string
    ,cnts INT
    ) partitioned BY (
    day string
    ,dim string
    );

-- 利用多重insert语法来统计各种维度组合的日活用户数，并插入到日活维度统计表的各分区中；
FROM app.etl_user_active_day

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0000'
    )
SELECT 'all'
    ,'all'
    ,'all'
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1000'
    )
SELECT os_name
    ,'all'
    ,'all'
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0100'
    )
SELECT 'all'
    ,city
    ,'all'
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY city

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0010'
    )
SELECT 'all'
    ,'all'
    ,release_channel
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY release_channel

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0001'
    )
SELECT 'all'
    ,'all'
    ,'all'
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY app_ver_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1100'
    )
SELECT os_name
    ,city
    ,'all'
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name, city

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1010'
    )
SELECT os_name
    ,'all'
    ,release_channel
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name, release_channel

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1001'
    )
SELECT os_name
    ,'all'
    ,'all'
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name, app_ver_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0110'
    )
SELECT 'all'
    ,city
    ,release_channel
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY city, release_channel

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0101'
    )
SELECT 'all'
    ,city
    ,'all'
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY city, app_ver_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0011'
    )
SELECT 'all'
    ,'all'
    ,release_channel
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY release_channel, app_ver_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1110'
    )
SELECT os_name
    ,city
    ,release_channel
    ,'all'
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name, city, release_channel

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1101'
    )
SELECT os_name
    ,city
    ,'all'
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name, city, app_ver_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1011'
    )
SELECT os_name
    ,'all'
    ,release_channel
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name, release_channel, app_ver_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '0111'
    )
SELECT 'all'
    ,city
    ,release_channel
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY city, release_channel, app_ver_name

INSERT INTO TABLE app.dim_user_active_day PARTITION (
    day = '2017-08-14'
    ,dim = '1111'
    )
SELECT os_name
    ,city
    ,release_channel
    ,app_ver_name
    ,count(1)
WHERE day = '2017-08-14'
GROUP BY os_name, city, release_channel, app_ver_name
;
