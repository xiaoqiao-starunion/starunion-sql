/*
1.需求：联盟进化参数数据
2.需求目的：分析联盟科技更新前后参与情况
3.筛选条件
自然时间：8.17 -10.9 ，唯一设备id，等级大于8级的玩家
4.数据结构
日期，DAU，去新DAU，进行联盟进化捐献玩家数，进行联盟进化捐献次数
*/

-- 基础玩家数据 


WITH users AS (
    SELECT
        u.uuid,
        u.reg_date,
        login.login_date 
    FROM
        ( SELECT uuid, DATE_FORMAT( FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d' ) reg_date 
            FROM roles WHERE server_id  != '1' AND is_internal = FALSE 
            ) u
        LEFT JOIN (
        SELECT DISTINCT
            role_id,
            DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) login_date 
        FROM
            log_login_role 
        WHERE
            created_at >= 1629158400 
            AND created_at < 1633824000 AND YEAR = 2021 AND MONTH >= 8 
            AND base_level > 8 
        ) login ON u.uuid = login.role_id
        INNER JOIN ( 
            SELECT DISTINCT role_id 
            FROM 
            ( SELECT role_id, row_number() over ( PARTITION BY device_id ORDER BY base_level desc) level_rank 
                FROM log_login_role ) w 
            WHERE w.level_rank = 1 ) max_level 
        ON u.uuid = max_level.role_id
  
    
)

SELECT
    dau_info.login_date,
    dau_info.dau,
    reg.reg_cnt,
    present.present_cnt,
    present.present_times 
FROM
    ( SELECT login_date, count( DISTINCT uuid ) dau -- DAU
    FROM users 
    WHERE login_date IS NOT NULL 
    GROUP BY login_date 
    ) dau_info
    LEFT JOIN ( 
    SELECT login_date, count( DISTINCT uuid ) reg_cnt -- 新注册玩家数
    FROM users 
    WHERE reg_date = login_date GROUP BY login_date 
    ) reg ON dau_info.login_date = reg.login_date
    LEFT JOIN (
    SELECT
        u.login_date,
        count( DISTINCT res.role_id ) present_cnt, -- 捐献玩家数 
        sum(present_times) present_times -- 捐献次数 
    FROM
        ( SELECT DISTINCT uuid,login_date FROM users WHERE login_date IS NOT NULL ) u
        INNER JOIN (
            SELECT role_id,count(role_id) as present_times,res_date
            FROM
            (
        SELECT
            role_id,
            DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) res_date 
        FROM
            log_resource 
        WHERE
            created_at >= 1629158400 
            AND created_at < 1633824000 AND YEAR = 2021 AND MONTH >= 8 
        AND action IN ( '1010', '1011' )
        ) as tmp group by role_id,res_date
            )res ON u.uuid = res.role_id 
        AND u.login_date = res.res_date 
    GROUP BY
        u.login_date 
    ) present ON dau_info.login_date = present.login_date;