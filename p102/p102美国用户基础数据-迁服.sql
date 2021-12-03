美国用户基础数据-迁服
需求目的：分析长线留存和迁服效果
筛选条件：3月至今所有美国用户，9.6-9.10迁服后的玩家归属到原服务器
数据格式：
日期、注册账号数、新增角色数、dau、唯一设备dau、付费dau、充值人数、充值金额、arpu、arppu、新增首充角色书
汇总、分服

--这种计算方式会把在9.6-9.10之间没有参与迁服活动但是新手阶段迁进来的玩家算进去，比如9.6=9.10，从5-10，没有参加活动，会把他的原始服判定为10.


with users as
(
select uuid as role_id,if(last_server_id is not null,last_server_id,server_id) as server_id,country,created_at,account_id
from
(
select uuid,server_id,country,created_at,account_id -- 每个用户的最新ID
from roles
where is_internal = false and server_id!='1' and country='US'
) as t1
left join
(
select  role_id,last_server_id,server_id as new_server,
DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%I:%S' ) transfer_date
	from log_fly
	where fly_category=204147
	and created_at >=1630886400 and  created_at <1631318400

) as t2 on t1.uuid=t2.role_id
)



SELECT active_user.daily AS daily ,
  active_user.server_id AS server_id,
  COALESCE ( account_create_num.val, 0 ) "新增账户数",
	COALESCE ( role_create_num.val, 0 ) "创角数",
	COALESCE( active_user.val, 0 ) DAU,
	COALESCE( active_device.val, 0 ) "唯一设备dau",
    COALESCE ( active_paid_user.val, 0 ) "付费DAU",
	COALESCE ( paying_user.val, 0 ) "充值人数" ,
	COALESCE ( paying_amount.val, 0 ) "充值金额",
	COALESCE ( paying_amount.val, 0 )/ COALESCE ( paying_user.val, null)"ARPPU",
	COALESCE ( paying_amount.val, 0 )/COALESCE( active_user.val, null )"ARPU",
	COALESCE ( first_paid.val, 0 ) "新增首充用户数"

-- COALESCE ( level_zero.val, 0 ) "0级角色数",
-- 	COALESCE ( account_level_zero.val, 0 ) "0级账户数",
-- 	COALESCE ( paying_times.val, 0 ) "充值次数",
	
FROM
	(
	SELECT
		users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id ) val 
	FROM
		log_login_role
		inner join users on users.role_id = log_login_role.role_id
		
	
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_user
	left join
	(
	SELECT
		users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.device_id ) val 
	FROM
		log_login_role
		inner join users on users.role_id = log_login_role.role_id
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_device 
	ON active_device.server_id = active_user.server_id 
	AND active_device.daily = active_user.daily
	LEFT JOIN (
	SELECT
		accounts.server_id,
		DATE_FORMAT( FROM_UNIXTIME( accounts.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT accounts.uuid ) val 
	FROM
		accounts inner join users on users.account_id = accounts.uuid
	
	WHERE
	 accounts.uuid NOT IN ( SELECT account_id FROM roles WHERE is_internal = TRUE GROUP BY account_id ) 

	GROUP BY
		accounts.server_id,
	DATE_FORMAT( FROM_UNIXTIME( accounts.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	account_create_num 
	ON account_create_num.server_id = active_user.server_id 
	AND account_create_num.daily = active_user.daily
	LEFT JOIN (
	SELECT
		users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id ) val 
	FROM
		log_login_role
		inner join users on users.role_id=log_login_role.role_id
		
		INNER JOIN (
		SELECT
			min( payments.created_at ) first_paid_at,
			payments.role_id 
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
		WHERE
			payments.is_paid = 1 
			AND payments.STATUS = 2 
			AND payments.is_test = 2 
			AND payments.pay_type != 3 
			AND payments.pay_channel != 'MYCARD' 
		GROUP BY
			payments.role_id 
		) temp ON temp.role_id = log_login_role.role_id 

		AND (
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( temp.first_paid_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))>= 0 
		) 
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_paid_user 
	ON active_paid_user.server_id = active_user.server_id 
	AND active_paid_user.daily = active_user.daily
	
	

	LEFT JOIN (
			SELECT
				users.server_id,
				DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				round( sum( payments.price ), 2 ) val 
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
				inner join users on users.role_id = payments.role_id
			
			WHERE
				
				 ( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
			GROUP BY
				users.server_id,
			DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) paying_amount ON paying_amount.server_id = active_user.server_id 
			AND paying_amount.daily = active_user.daily
	
	LEFT JOIN (
			SELECT
				users.server_id,
				DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				count( DISTINCT payments.role_id ) val 
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
				
			WHERE
				
				 ( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
				
			GROUP BY
				users.server_id,
			DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) paying_user 
	      ON paying_user.server_id = active_user.server_id 
			AND paying_user.daily = active_user.daily
	LEFT JOIN (
			SELECT
				users.server_id,
				DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				count( DISTINCT log_login_role.role_id ) val 
			FROM
				log_login_role
				inner join users on users.role_id=log_login_role.role_id
				
			WHERE
				  (
				DATE_FORMAT( FROM_UNIXTIME( users.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )= DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			GROUP BY
				users.server_id,
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			register_active_user 
			ON register_active_user.server_id = active_user.server_id 
			AND register_active_user.daily = active_user.daily
  LEFT JOIN (
			SELECT
				users.server_id,
				DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				count( DISTINCT roles.uuid ) val 
			FROM
				roles
				INNER JOIN users on users.role_id=roles.uuid
				LEFT JOIN (
				SELECT
					log_login_role.* 
				FROM
					log_login_role
					
				
				)	log_login_role ON log_login_role.role_id = roles.uuid 
				
				AND (
				DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )= DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			GROUP BY
				users.server_id,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			role_create_num 
			ON role_create_num.server_id = active_user.server_id 
			AND role_create_num.daily = active_user.daily
			
				LEFT JOIN (
	SELECT
	users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT payments.role_id ) val 
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
		INNER JOIN (
		SELECT
			min( payments.created_at ) first_paid_at,
			payments.role_id 
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
		WHERE
			payments.is_paid = 1 
			AND payments.STATUS = 2 
			AND payments.is_test = 2 
			AND payments.pay_type != 3 
			AND payments.pay_channel != 'MYCARD' 
		GROUP BY
			payments.role_id 
		) temp ON temp.role_id = payments.role_id 
		AND temp.first_paid_at = payments.created_at
		LEFT JOIN users ON users.role_id = payments.role_id 
		
	WHERE
		( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
	
	GROUP BY
	users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) first_paid 
				ON first_paid.daily = active_user.daily and first_paid.server_id=active_user.server_id
	
		ORDER BY
		concat( cast( length( server_id) AS VARCHAR ), server_id ),daily DESC



--  不分服

SELECT active_user.daily AS daily ,
  
  COALESCE ( account_create_num.val, 0 ) "新增账户数",
	COALESCE ( role_create_num.val, 0 ) "创角数",
	COALESCE( active_user.val, 0 ) DAU,
	COALESCE( active_device.val, 0 ) "唯一设备dau",
    COALESCE ( active_paid_user.val, 0 ) "付费DAU",
	COALESCE ( paying_user.val, 0 ) "充值人数" ,
	COALESCE ( paying_amount.val, 0 ) "充值金额",
	COALESCE ( paying_amount.val, 0 )/ COALESCE ( paying_user.val, null)"ARPPU",
	COALESCE ( paying_amount.val, 0 )/COALESCE( active_user.val, null )"ARPU",
	COALESCE ( first_paid.val, 0 ) "新增首充用户数"

-- COALESCE ( level_zero.val, 0 ) "0级角色数",
-- 	COALESCE ( account_level_zero.val, 0 ) "0级账户数",
-- 	COALESCE ( paying_times.val, 0 ) "充值次数",
	
FROM
	(
	SELECT
		
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id ) val 
	FROM
		log_login_role
		inner join users on users.role_id = log_login_role.role_id
		
	
	GROUP BY
		
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_user
	left join
	(
	SELECT
		
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.device_id ) val 
	FROM
		log_login_role
		inner join users on users.role_id = log_login_role.role_id
	GROUP BY
		
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_device 
	ON 
	 active_device.daily = active_user.daily
	LEFT JOIN (
	SELECT
		
		DATE_FORMAT( FROM_UNIXTIME( accounts.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT accounts.uuid ) val 
	FROM
		accounts inner join users on users.account_id = accounts.uuid
	
	WHERE
	 accounts.uuid NOT IN ( SELECT account_id FROM roles WHERE is_internal = TRUE GROUP BY account_id ) 
	GROUP BY
		
	DATE_FORMAT( FROM_UNIXTIME( accounts.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	account_create_num 
	ON 
	 account_create_num.daily = active_user.daily
	LEFT JOIN (
	SELECT
		
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id ) val 
	FROM
		log_login_role
		inner join users on users.role_id=log_login_role.role_id
		
		INNER JOIN (
		SELECT
			min( payments.created_at ) first_paid_at,
			payments.role_id 
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
		WHERE
			payments.is_paid = 1 
			AND payments.STATUS = 2 
			AND payments.is_test = 2 
			AND payments.pay_type != 3 
			AND payments.pay_channel != 'MYCARD' 
		GROUP BY
			payments.role_id 
		) temp ON temp.role_id = log_login_role.role_id 

		AND (
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( temp.first_paid_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))>= 0 
		) 
	GROUP BY
	
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_paid_user 
	ON 
	 active_paid_user.daily = active_user.daily
	
	

	LEFT JOIN (
			SELECT
				
				DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				round( sum( payments.price ), 2 ) val 
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
				inner join users on users.role_id = payments.role_id
			
			WHERE
				
				 ( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
			GROUP BY
				
			DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) paying_amount 
	      ON 
			 paying_amount.daily = active_user.daily
	
	LEFT JOIN (
			SELECT
				
				DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				count( DISTINCT payments.role_id ) val 
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
				
			WHERE
				
				 ( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
				
			GROUP BY
				
			DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) paying_user 
	      ON 
			 paying_user.daily = active_user.daily
	LEFT JOIN (
			SELECT
				
				DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				count( DISTINCT log_login_role.role_id ) val 
			FROM
				log_login_role
				inner join users on users.role_id=log_login_role.role_id
				
			WHERE
				  (
				DATE_FORMAT( FROM_UNIXTIME( users.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )= DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			GROUP BY
				
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			register_active_user 
			ON 
			 register_active_user.daily = active_user.daily
  LEFT JOIN (
			SELECT
				
				DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				count( DISTINCT roles.uuid ) val 
			FROM
				roles
				INNER JOIN users on users.role_id=roles.uuid
				LEFT JOIN (
				SELECT
					log_login_role.* 
				FROM
					log_login_role
					
				
				)	log_login_role ON log_login_role.role_id = roles.uuid 
				
				AND (
				DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )= DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			GROUP BY
				
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			role_create_num 
			ON  role_create_num.daily = active_user.daily
				LEFT JOIN (
	SELECT
		DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT payments.role_id ) val 
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
		INNER JOIN (
		SELECT
			min( payments.created_at ) first_paid_at,
			payments.role_id 
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
		WHERE
			payments.is_paid = 1 
			AND payments.STATUS = 2 
			AND payments.is_test = 2 
			AND payments.pay_type != 3 
			AND payments.pay_channel != 'MYCARD' 
		GROUP BY
			payments.role_id 
		) temp ON temp.role_id = payments.role_id 
		AND temp.first_paid_at = payments.created_at
		inner JOIN users ON users.role_id = payments.role_id 

	WHERE
		( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
	
		
	GROUP BY
	DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) first_paid ON first_paid.daily = active_user.daily
	
			
		ORDER BY
		daily DESC