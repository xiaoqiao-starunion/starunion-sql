注册时间：09.21-09.22
地理位置：沙特，阿联酋，卡塔尔，科威特，巴林，阿曼【加一起就行】

按游戏语言【AR，EN】9,2
分别输出次留


SELECT
	* 
FROM
	(
	SELECT
		-- log_login_role.server_id AS log_login_role_server_id,
		log_login_role.daily AS log_login_role_daily,language,
		sum( CASE WHEN diff = 1 THEN 1 ELSE 0 END ) day2
	FROM
		(
		SELECT
			log_login_role.role_id,language,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) AS daily,
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) AS loginDay,
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' )) AS diff,
			log_login_role.server_id 
		FROM
			log_login_role
			INNER JOIN 
			(
			SELECT roles.uuid, roles.created_at ,language,country
			FROM roles
			 WHERE  roles.created_at >= 1632182400 
			AND roles.created_at <= 1632355200 
		AND roles.is_internal = FALSE and server_id!='1'
		and country in ('SA','AE','QA','KW','BH','OM')
		and language in (2,9)
		and device_os='Android'
		 ) roles ON roles.uuid = log_login_role.role_id 
		WHERE	
			 YEAR = 2021 AND MONTH = 9 
		GROUP BY
			log_login_role.role_id,language,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ),
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ),
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' )),
			log_login_role.server_id 
		) log_login_role 
	GROUP BY
	--	log_login_role.server_id,
		log_login_role.daily ,language
	) AS temp1
	RIGHT JOIN (
	SELECT
	--	roles.server_id,
	language,
		DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count(*) create_num 
	FROM
		roles 
	WHERE
	roles.created_at >= 1632182400 
			AND roles.created_at <= 1632355200 
		AND roles.is_internal = FALSE and server_id!='1'
		and country in ('SA','AE','QA','KW','BH','OM')
		and language in (2,9)
		and device_os='Android'
	GROUP BY
	--	roles.server_id,
		language,
	DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) AS temp2 ON temp1.log_login_role_daily = temp2.daily 
	--AND temp1.log_login_role_server_id = temp2.server_id 
	and temp1.language = temp2.language
ORDER BY
	daily