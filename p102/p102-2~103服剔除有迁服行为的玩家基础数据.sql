p102-2~103服剔除有迁服行为的玩家基础数据.sql
迁服行为指9.6~9.10迁城类型为204147的玩家fly_category
时间：8月1日~9月22日
格式：服务器、时间、DAU、付费DAU、充值人数、付费率、ARPU、ARPPU、14天静默付费人数 @小乔 



with users as
(
SELECT uuid as role_id,server_id,last_paid_day,paid
FROM
(
select  uuid,server_id,paid
	,DATE_FORMAT( FROM_UNIXTIME( paid_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) last_paid_day
from roles
where is_internal = false and server_id!='1'
) as t1
left join
( select distinct role_id
	from log_fly
	where fly_category=204147
	and created_at >=1630886400 and  created_at <1631318400
) as t2 on t1.uuid=t2.role_id
where t2.role_id is null
)





SELECT active_user.daily AS daily ,
  active_user.server_id AS server_id,

	COALESCE( active_user.val, 0 ) DAU,
	COALESCE ( active_paid_user.val, 0 ) "付费DAU",
	COALESCE ( paying_user.val, 0 ) "充值人数" ,
	COALESCE ( paying_amount.val, 0 ) "充值金额",
	COALESCE ( paying_user.val, 0 )/COALESCE( active_user.val, null) "DAU付费率",
	COALESCE ( paying_amount.val, 0 )/ COALESCE ( paying_user.val, null)"ARPPU",
	COALESCE ( paying_amount.val, 0 )/COALESCE( active_user.val, null )"ARPU",
	COALESCE ( active_paid_silence.val, 0 )"14天静默付费用户数"
	

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
		where year=2021 and month>=7
		and created_at>=1627776000
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_user
	left join (
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
	WHERE
		   year=2021 and month>=7 and log_login_role.created_at>=1627776000
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
				 payments.created_at>=1627776000
				 and ( payments.is_paid = 1 AND payments.STATUS = 2 AND payments.is_test = 2 AND payments.pay_type != 3 AND payments.pay_channel != 'MYCARD' ) 
				
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
				 and created_at>=1627776000
			GROUP BY
				users.server_id,
			DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
			) 
	paying_user ON paying_user.server_id = active_user.server_id 
			AND paying_user.daily = active_user.daily
	
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
					
				where year=2021 and month>=7 and created_at>=1627776000
				)	log_login_role ON log_login_role.role_id = roles.uuid 
				where roles.is_internal = FALSE 
				AND (
				DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )= DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			GROUP BY
				users.server_id,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			role_create_num 
			ON role_create_num.server_id = active_user.server_id 
			AND role_create_num.daily = active_user.daily
left join ( 
	SELECT server_id,daily,val
	from
	(
	select users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id ) val 
	
 		from log_login_role
 		inner join users on users.role_id=log_login_role.role_id
 		where year=2021 and month>=7 and log_login_role.created_at>=1627776000 
 		and DATE_DIFF ( 'day', DATE_PARSE ( last_paid_day, '%Y-%m-%d' ),
						 DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d'  ))>=14
 		group by users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) 
		

 		) as login 
)as active_paid_silence--付费静默 
on active_paid_silence.server_id=active_user.server_id  AND active_paid_silence.daily = active_user.daily
			
		ORDER BY
		concat( cast( length( server_id ) AS VARCHAR ), server_id ),daily DESC