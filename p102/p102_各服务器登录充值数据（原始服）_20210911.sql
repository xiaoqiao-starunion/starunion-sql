时间：3.8-9.10
迁服后的玩家归属到原服务器（2-103），其余服务器没开迁徙
表结构：
服务器，日期，dau，付费dau，当日付费人数，当日收入，当日arppu @动人 
with users as
(
select role_id,server_id,country
from(
select role_id,server_id,country,ROW_NUMBER() over(partition by role_id order by try_cast(server_id as integer)) as rank
from
(
SELECT role_id,server_id,country
FROM
(
select  role_id,server_id,created_at, country,-- 玩家第一次登陆的服务器ID
			ROW_NUMBER() over(partition by role_id order by created_at) as rank
from log_login_role
where role_id in (select uuid from roles where is_internal = false and server_id!='1')
and try_cast(server_id as integer) between 2 and 103
) as t1 where rank=1

union all

SELECT role_id,server_id,country 
FROM
(
select  role_id,server_id,created_at, country
			
from log_login_role
where role_id in (select uuid from roles where is_internal = false and server_id!='1')
and try_cast(server_id as integer) > 103
) as t2
) as tmp 
) as tmp2 where rank=1
) 


select t1.log_day "日期","登录人数" as "dau",device_uv as "唯一设备dau","总充值人数",active_paid_uv as "付费dau"
				,"总充值金额",arpu,arppu
from
(
select log_day ,
		count(distinct log.role_id) as "登录人数",--log_uv,
		count(distinct pay.role_id) as "总充值人数",-- pay_uv,
		sum(price) as "总充值金额",-- pay_total,
		sum(price)/count(distinct log.role_id) arpu,
		sum(price)/count(distinct pay.role_id) arppu

		
from 
(
select role_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as log_day
from log_login_role 
where created_at>= 1615161600 and created_at<1631318400 and server_id!='1'
		and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') 

) as log
left join (
	select role_id,sum(price) price,
			DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as pay_day
	FROM payments
	where created_at>= 1615161600 and created_at<1631318400
		and is_paid=1
		and is_test=2
		and status=2
	group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')
)as pay
on log.role_id=pay.role_id and log_day=pay_day
group by log_day
)as t1
left join
(	
SELECT
		
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id )  active_paid_uv--付费dau
	FROM
		log_login_role
		-- inner join users on users.role_id=log_login_role.role_id
		INNER JOIN (
		SELECT
			min( payments.created_at ) first_paid_at,
			payments.role_id 
		FROM
			(
			
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
					
			) payments 
		WHERE
			payments.is_paid = 1 
			AND payments.STATUS = 2 
			AND payments.is_test = 2 
			
			
		GROUP BY
			payments.role_id 
		) temp ON temp.role_id = log_login_role.role_id 
	WHERE
		
		 (
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( temp.first_paid_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))>= 0 
		) 
	GROUP BY
	
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
	)  as t2 on t1.log_day=t2.daily
left join(
	select count(distinct device_id) as device_uv,log_day
	 from
(
select device_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as log_day
from log_login_role 
where created_at>= 1615161600 and created_at<1631318400 and server_id!='1'
		and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by device_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') 
		) as tmp group by log_day

)as t3 on t3.log_day=t1.log_day





select t1.server_id,t1.log_day as "日期","登录人数" as "dau",device_uv as "唯一设备dau"
				,"总充值人数",active_paid_uv as "付费dau"
				,"总充值金额",arpu,arppu
from 
(
select users.server_id,log_day,
		count(distinct log.role_id) as "登录人数", --log_uv,
		count(distinct pay.role_id) as "总充值人数",--pay_uv,
		sum(price) as "总充值金额",-- pay_total,
		sum(price) / count(distinct log.role_id) as arpu,
		sum(price)/count(distinct pay.role_id) as arppu

		
from 
(
select role_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as log_day
from log_login_role 
where created_at>= 1615161600 and created_at<1631318400 and server_id!='1'
		and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') 

) as log
inner join users on users.role_id=log.role_id
left join (
	select role_id,sum(price) price,
			DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as pay_day
	FROM payments
	where created_at>= 1615161600 and created_at<1631318400
		and is_paid=1
		and is_test=2
		and status=2
	group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')
)as pay
on log.role_id=pay.role_id and log_day=pay_day
group by log_day,users.server_id
)t1
left join
(
SELECT
		users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id )  active_paid_uv--付费dau
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
						
					
			) payments 
		WHERE
			payments.is_paid = 1 
			AND payments.STATUS = 2 
			AND payments.is_test = 2 
			
		GROUP BY
			payments.role_id 
		) temp ON temp.role_id = log_login_role.role_id 
	WHERE
		
		 (
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( temp.first_paid_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ), '%Y-%m-%d' ))>= 0 
		) 
	GROUP BY
		users.server_id,
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )

)as active_paid_user on active_paid_user.server_id=t1.server_id and active_paid_user.daily=t1.log_day
left join(
	select count(distinct device_id) as device_uv,log_day,server_id
	 from
(
select device_id,users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as log_day
from log_login_role 
inner join users on users.role_id=log_login_role.role_id
where created_at>= 1615161600 and created_at<1631318400 and users.server_id!='1'
		
group by device_id,users.server_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') 
		) as tmp group by log_day,server_id

)as t3 on t3.log_day=t1.log_day and t3.server_id=t1.server_id