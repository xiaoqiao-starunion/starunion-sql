2021-08-02

1.连续充值
分析维度：购买玩家人群分析，续购情况分析
平台数据需求：
7.26号之后购买礼包ID为2915801-2915805的玩家
玩家ID，区服，注册时间，充值时间，充值时等级，购买礼包ID，
历史付费金额(充2915801之前的)，历史付费次数，购买礼包2915801前最后一次充值时间
SELECT
	uuid,
	server_id,
	pay.base_level,
	DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%i:%s' ) cre_at,
	DATE_FORMAT( FROM_UNIXTIME( pay.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%i:%s' ) pay_at,
	conf_id,
	paid.paid_total,
	paid.paid_times,
	DATE_FORMAT( FROM_UNIXTIME( paid.last_pay_at ) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%i:%s' ) last_paid_at
FROM
	roles
	INNER JOIN (
	SELECT
		role_id,
		created_at,
		base_level,
		conf_id,
		row_number() over ( PARTITION BY role_id ORDER BY created_at DESC ) AS rank 
	FROM
		payments 
	WHERE
		is_paid = 1 
		AND is_test = 2 
		AND STATUS = 2 
	) AS pay ON roles.uuid = pay.role_id
	LEFT JOIN (
	SELECT
		payments.role_id,
		sum( price ) as paid_total,
		count( payments.role_id ) as paid_times,
        max(last_pay_at) as last_pay_at
		
	FROM
		payments
		INNER JOIN (
		SELECT
			t1.role_id,
			t1.created_at as pay_at_01,
			t2.created_at as last_pay_at --01前一次充值的时间
		FROM
			(
			SELECT
				role_id,
				created_at,
				rank 
			FROM
				(
				SELECT
					role_id,
					conf_id,
					created_at,
					row_number() over ( PARTITION BY role_id ORDER BY created_at ) AS rank 
				FROM
					payments 
				WHERE
					is_paid = 1 
					AND is_test = 2 
					AND STATUS = 2 
				) AS a1 
			WHERE
				conf_id = 2915801 
			) AS t1
			LEFT JOIN (
			
				SELECT
					role_id,
					conf_id,
          created_at,
					row_number() over ( PARTITION BY role_id ORDER BY created_at ) AS rank 
				FROM
					payments 
				WHERE
					is_paid = 1 
					AND is_test = 2 
					AND STATUS = 2 
			
			) t2 ON t1.role_id = t2.role_id 
		WHERE
			t2.rank = t1.rank - 1 
		) AS pay_01 ON payments.role_id = pay_01.role_id 
	WHERE
		payments.created_at < pay_01.pay_at_01 
		AND is_paid = 1 
		AND is_test = 2 
		AND STATUS = 2 
		
	GROUP BY
	payments.role_id 
	) AS paid ON  roles.uuid= paid.role_id
	where  roles.is_internal =false and server_id!='1' 
	and pay.conf_id in (2915801,2915802,2915803,2915804,2915805)
	and pay.created_at >= 1627257600