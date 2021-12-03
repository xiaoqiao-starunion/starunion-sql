/*
需求：细胞相关参与情况
需求目的：分析细胞相关参与情况
筛选条件：筛选时间：9.27-10.7，按设备ID
数据结构：
日期，DAU（大于等于10级），建造了相关建筑（变异菌丛和地下洞穴）的玩家数，参与地下洞穴玩法的玩家数，激活了6个细胞的玩家数
*/ -- 基础用户信息


WITH users AS
  (SELECT u.uuid,
          u.base_level,
          u.login_date
   FROM
     (SELECT distinct uuid,
             logout.base_level,login_date
      FROM roles 
      inner join 
			(select role_id,server_id,base_level, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date,
				row_number() over(partition by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') order by created_at desc) as rank --每一天最后一次登出时等级
			from log_logout
			WHERE created_at >= 1632700800 AND created_at < 1633651200
			AND YEAR = 2021
        	AND MONTH >= 9
			)as logout on logout.role_id=roles.uuid
      WHERE roles.server_id != '1'
        AND is_internal = FALSE
        and rank=1 and logout.base_level>=10
        ) u
   INNER JOIN
     (SELECT DISTINCT role_id
      FROM
        (SELECT role_id,
                server_id,
                base_level,
                row_number() over(partition BY device_id ORDER BY base_level DESC) level_rank
         FROM log_login_role) m
      WHERE level_rank = 1) max_level ON u.uuid = max_level.role_id
     
   )
     -- 统计


SELECT u.login_date,
       count(DISTINCT u.uuid) AS DAU,
       count(DISTINCT build.role_id) AS build_cnt,
       count(DISTINCT log_p.role_id) AS under_cavern_player
FROM users u
LEFT JOIN
  (SELECT DISTINCT role_id,
                   build_conf_id,
                   DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') b_date
   FROM log_buildings 
   WHERE build_conf_id IN (4093000,
                           4094000)
     AND action IN ('6201',
                    '6202')
     AND created_at >= 1632700800
     AND created_at < 1633651200
     AND YEAR = 2021
        	AND MONTH >= 9) build ON u.uuid = build.role_id AND u.login_date = build.b_date
LEFT JOIN
  (SELECT DISTINCT role_id,
                   DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') p_date
   FROM log_puzzle
   WHERE created_at >= 1632700800
     AND created_at < 1633651200
     AND YEAR = 2021
        	AND MONTH >= 9) log_p ON u.uuid = log_p.role_id AND u.login_date = log_p.p_date
GROUP BY u.login_date;
