付费分层-玩家每日数据
select a.*,b.* 
from(
select pay_day,sum(price) as pay_total,--当天充值总额
			count(distinct role_id) as pay_uv,--当天充值人数
			sum(if(user_type='破冰用户',price,0)) as "破冰用户充值金额",
			sum(if(user_type='小R',price,0)) as "小R充值金额",
			sum(if(user_type='中R',price,0)) as "中R充值金额",
			sum(if(user_type='大R',price,0)) as "大R充值金额",
			count(if(user_type='破冰用户',role_id,null)) as "破冰用户充值人数",
			count(if(user_type='小R',role_id,null)) as "小R充值人数",
			count(if(user_type='中R',role_id,null)) as "中R充值人数",
			count(if(user_type='大R',role_id,null)) as "大R充值人数"
from(
SELECT role_id,pay_day,
		price,--当天充值金额
		(his_price-price) as his_paid --每个玩家当天以前的充值总额
		,case when (his_price-price) = 0 then'破冰用户'
		when (his_price-price) >0 and (his_price-price) <= 200 then'小R'
		when (his_price-price) >200 and (his_price-price) <= 1000 then'中R'
		when (his_price-price) >1000 then'大R'
		end as user_type
FROM
(
SELECT role_id,pay_day,price,sum(price) over(partition by role_id order by pay_day ) as his_price
FROM
(
SELECT role_id,sum( payments.price ) price ,
		DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as pay_day
FROM
	(
	(
	SELECT
		role_id,
		created_at,
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
		) 
	UNION
	(
	SELECT
		role_id,
		verify_at AS created_at,
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
	WHERE
		type IN ( 3, 1 ) 
		AND is_statistics IN ( 1, 3 ) 
		AND account_balance_recharges.STATUS = 3 
	)
) payments  INNER JOIN roles ON roles.uuid = payments.role_id 
where is_internal=false and server_id!='1' 
	and ( payments.is_paid = 1 AND payments.STATUS = 2 
		AND payments.is_test = 2 AND payments.pay_type != 3 
		AND payments.pay_channel != 'MYCARD' ) 
	    and payments.created_at>=1615161600 and roles.created_at>=1615161600

group by role_id,DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)
) 
) group by pay_day
) as a 
left join (

	select daily,
			count(if(user_type='小R',uuid,null)) as "小R人数",
			count(if(user_type='中R',uuid,null)) as "中R人数",
			count(if(user_type='大R',uuid,null)) as "大R人数"
	from(
	SELECT daily,  
        uuid,
		case 
		when (his_paid) >0 and (his_paid) <= 200 then'小R'
		when (his_paid) >200 and (his_paid) <= 1000 then'中R'
		when (his_paid) >1000 then'大R'
		end as user_type
FROM
(
 select daily,role_id as uuid,
			sum(price) as his_paid --每个玩家截止每天的累计充值金额
	FROM
    (  
	     SELECT
			date_format( x, '%Y-%m-%d' ) daily,
			cast( to_unixtime ( x ) AS INTEGER ) daily_tag 
		FROM
			unnest (
			sequence ( cast ( '2021-03-08' AS date ), CURRENT_DATE, INTERVAL '1' DAY )) t ( x )
    ) as t1 
   left join
	 (
	SELECT role_id,sum( payments.price ) price ,
			DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as pay_day
	FROM
		(
		(
		SELECT
			role_id,
			created_at,
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
			) 
		UNION
		(
		SELECT
			role_id,
			verify_at AS created_at,
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
		WHERE
			type IN ( 3, 1 ) 
			AND is_statistics IN ( 1, 3 ) 
			AND account_balance_recharges.STATUS = 3 
		)
	) payments  INNER JOIN roles ON roles.uuid = payments.role_id 
	where is_internal=false and server_id!='1' 
		and ( payments.is_paid = 1 AND payments.STATUS = 2 
			AND payments.is_test = 2 AND payments.pay_type != 3 
			AND payments.pay_channel != 'MYCARD' ) 
		    and payments.created_at>=1615161600 and roles.created_at>=1615161600
	group by role_id,DATE_FORMAT( FROM_UNIXTIME( payments.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
	) as t2  on 1=1
	 where pay_day<daily
	group by  daily,role_id
)
) group by daily
)as b on a.pay_day=b.daily 




