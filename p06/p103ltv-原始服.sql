p103ltv-原始服




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
	temp1.*,
	temp2.create_num,
	temp2.daily AS dateField,
	temp2.server_id AS role_server_id 
FROM
	(
	SELECT
		users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		round(
			sum(
			IF
				(
					DATE_DIFF (
						'day',
						DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
					DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))<= 1-1,
					payments.price,
					0 
				)),
			2 
		) LTV1,
		round(
			sum(
			IF
				(
					DATE_DIFF (
						'day',
						DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
					DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))<= 3-1,
					payments.price,
					0 
				)),
			2 
		) LTV3,
		round(
			sum(
			IF
				(
					DATE_DIFF (
						'day',
						DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
					DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))<= 7-1,
					payments.price,
					0 
				)),
			2 
		) LTV7,
		round(
			sum(
			IF
				(
					DATE_DIFF (
						'day',
						DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
					DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))<= 15-1,
					payments.price,
					0 
				)),
			2 
		) LTV15,
		round(
			sum(
			IF
				(
					DATE_DIFF (
						'day',
						DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
					DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))<= 30-1,
					payments.price,
					0 
				)),
			2 
		) LTV30 
	FROM
		(
		SELECT
			* 
		FROM
			((
				SELECT
					role_id,
					account_id,
					created_at,
					server_id,
					country,
					price,
					is_paid,
					is_test,
					STATUS,
					pay_type,
					pay_channel,
					device_os,
					device_id,
					base_level,
					uuid AS payment_uuid 
				FROM
					payments 
					) UNION
				(
				SELECT
					role_id,
					roles.account_id,
					verify_at AS created_at,
					server_id,
					country,
					usd_amount AS price,
				IF
					( is_statistics = 1 OR is_statistics = 3, 1, 0 ) AS is_paid,
					2 AS is_test,
					2 AS STATUS,
					2 AS pay_type,
					pay_channel,
					'' AS device_os,
					'' AS device_id,
					0 AS base_level,
					'' AS payment_uuid 
				FROM
					account_balance_recharges
					INNER JOIN roles ON roles.uuid = account_balance_recharges.role_id 
				WHERE
					type IN ( 3, 1 ) 
					AND is_statistics IN ( 1, 3 ) 
					AND account_balance_recharges.STATUS = 3 
				)) temp_payment 
		) payments
		inner join users on users.role_id=payments.role_id
		LEFT JOIN roles ON roles.uuid = payments.role_id 
	WHERE
		
		 ( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
		AND roles.is_internal = FALSE 
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )

	) temp1
	RIGHT JOIN (
	SELECT
		users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count(*) create_num 
	FROM
		roles 
		inner join users on users.role_id=roles.uuid
	WHERE
		
		 roles.is_internal = FALSE 
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) temp2 ON temp1.daily = temp2.daily 
	AND temp1.server_id = temp2.server_id 
ORDER BY
	dateField