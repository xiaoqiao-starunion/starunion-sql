p103--留存-原始服


with users as
(
SELECT role_id,server_id,country
FROM
(
select  role_id,server_id,created_at, country,-- 玩家第一次登陆的服务器ID
			ROW_NUMBER() over(partition by role_id order by created_at) as rank
from log_login_role
where role_id in (select uuid from roles where is_internal = false)
) as t where rank=1
)



SELECT
	* 
FROM
	(
	SELECT
		log_login_role.server_id AS log_login_role_server_id,
		log_login_role.daily AS log_login_role_daily,
		sum( CASE WHEN diff = 1 THEN 1 ELSE 0 END ) day2,
		sum( CASE WHEN diff = 2 THEN 1 ELSE 0 END ) day3,
		sum( CASE WHEN diff = 6 THEN 1 ELSE 0 END ) day7,
		sum( CASE WHEN diff = 14 THEN 1 ELSE 0 END ) day15,
		sum( CASE WHEN diff = 29 THEN 1 ELSE 0 END ) day30,
		sum( CASE WHEN diff = 59 THEN 1 ELSE 0 END ) day60,
		sum( CASE WHEN diff = 89 THEN 1 ELSE 0 END ) day90,
		sum( CASE WHEN diff = 119 THEN 1 ELSE 0 END ) day120,
		sum( CASE WHEN diff = 149 THEN 1 ELSE 0 END ) day150,
		sum( CASE WHEN diff = 179 THEN 1 ELSE 0 END ) day180 
	FROM
		(
		SELECT
			log_login_role.role_id,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) AS daily,
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) AS loginDay,
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' )) AS diff,
			users.server_id 
		FROM
			log_login_role
			INNER join users on users.role_id = log_login_role.role_id
			INNER JOIN ( SELECT roles.uuid, roles.created_at FROM roles WHERE roles.is_internal = FALSE ) roles ON roles.uuid = log_login_role.role_id 
		
			
		GROUP BY
			log_login_role.role_id,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ),
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ),
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' )),
			users.server_id 
		) log_login_role 
	GROUP BY
		server_id,
		log_login_role.daily 
	) AS temp1
	RIGHT JOIN (
	SELECT
		users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count(*) create_num 
	FROM
		roles 
		inner join users on users.role_id = roles.uuid
	WHERE
	
		 roles.is_internal = FALSE 
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) AS temp2 
	ON temp1.log_login_role_daily = temp2.daily 
	AND temp1.log_login_role_server_id = temp2.server_id 
ORDER BY
	daily