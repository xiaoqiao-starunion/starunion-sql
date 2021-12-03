提数需求一（申请加急）：
服务器总体数据：不重复总登录人数、总充值人数、总充值金额、arpu
分服数据：登录人数、充值人数、充值金额、arpu
查询服务器：2-323
查询时间：3.8 00:00:00-8.25 23:59:59（UTC+0）


提数需求二（申请加急）：
分服数据：开服时间、新增人数、充值金额、充值人数
查询服务器：2-323
查询时间：3.8 00:00:00-8.25 23:59:59（UTC+0） 



select log_day "日期",
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
where created_at>= 1615161600 and created_at<1631232000 and server_id!='1'
		and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') 

) as log
left join (
	select role_id,sum(price) price,
			DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as pay_day
	FROM payments
	where created_at>= 1615161600 and created_at<1631232000
		and is_paid=1
		and is_test=2
		and status=2
	group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')
)as pay
on log.role_id=pay.role_id and log_day=pay_day
group by log_day




select server_id,log_day,
		count(distinct log.role_id) as "登录人数", --log_uv,
		count(distinct pay.role_id) as "总充值人数",--pay_uv,
		sum(price) as "总充值金额",-- pay_total,
		sum(price) / count(distinct log.role_id) as arpu,
		sum(price)/count(distinct pay.role_id) as arppu

		
from 
(
select role_id,server_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as log_day
from log_login_role 
where created_at>= 1615161600 and created_at<1631232000 and server_id!='1'
		and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,server_id,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') 

) as log
left join (
	select role_id,sum(price) price,
			DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as pay_day
	FROM payments
	where created_at>= 1615161600 and created_at<1631232000
		and is_paid=1
		and is_test=2
		and status=2
	group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')
)as pay
on log.role_id=pay.role_id and log_day=pay_day
group by log_day,server_id





select  servers.uuid as server_id
		,DATE_FORMAT( FROM_UNIXTIME( servers.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') as server_open_day
		,reg_uv,paid_uv,price
FROM servers
inner join
(select count(distinct uuid) as reg_uv,server_id
from roles
where  created_at>= 1615161600 and created_at<1631232000
and is_internal=false and server_id!='1'
group by server_id
)as reg on reg.server_id=servers.uuid
inner join 
( select server_id,sum(price) as price,count(distinct role_id) as paid_uv
	FROM payments
	where created_at>= 1615161600 and created_at<1631232000
		and is_paid=1
		and is_test=2
		and status=2
	group by server_id
)as pay on pay.server_id=servers.uuid




SELECT case when total_price<100000 then '10万美金以下'
						when total_price>=100000 and total_price<200000 then '10万-20万美金'
						when total_price>=200000 and total_price<300000 then '20万-30万美金'
						when total_price>=300000 and total_price<400000 then '30万-40万美金'
						when total_price>=400000 then '40万美金+'
						else 'qita' end as type,count(distinct server_id)
FROM
(
select sum(price) as total_price,server_id
FROM
(
SELECT uuid,DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) AS reg_day
FROM roles
where created_at>=1615161600 and created_at<1631232000
and is_internal=false and server_id!='1'
)as REG
left join
(
select role_id,server_id,sum(price) as price
FROM payments
where is_paid=1
and status=2
and is_test=2
and created_at>=1615161600 and created_at<1631232000 and server_id!='1'
group by role_id,server_id
)as paid
on reg.uuid=paid.role_id

group by server_id

)group by case when total_price<100000 then '10万美金以下'
						when total_price>=100000 and total_price<200000 then '10万-20万美金'
						when total_price>=200000 and total_price<300000 then '20万-30万美金'
						when total_price>=300000 and total_price<400000 then '30万-40万美金'
						when total_price>=400000 then '40万美金+'
						else 'qita' end 
						
						

