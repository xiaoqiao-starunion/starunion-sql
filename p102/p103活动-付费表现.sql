@小乔  大佬，麻烦一个数据需求
1、需求：
id为2010701和2010601的两个活动在结束后，不同积分区间的玩家的付费表现
2、需求目的：
分析S15和S16的新服活动的付费表现
3、筛选条件：
服务器：S15、S16
注册时间：UTC 10.21 00:00:00 - UTC 10.25 23:59:59
去内玩
4、数据格式



select  ac_id,type,server_id,count(distinct uuid) uv, count(distinct role_id) paid_users, count(distinct role_id)/count(distinct uuid) pr
from
( select uuid,server_id,ac_id,type,role_id,sum_price
	from(
	select u.uuid,server_id,ac_id,type
	from (
		select uuid, server_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') reg_date
	from roles
	where server_id in ('S15', 'S16') and is_internal = false
	and created_at >= 1634774400 and created_at < 1635206400
	) u 
	inner join (select role_id, ac_id,
				  	   case when balance between 0 and 60000 then 'p4'
				  	   when balance between 60001 and 240000 then 'p6'
				  	   when balance between 240001 and 500000 then 'p8'
				  	   when balance between 500001 and 800000 then 'p9'
				  	   when balance between 800001 and 1000000 then 'p10' 
				  	   when balance > 1000000 then 'p11' end type
				from (select role_id,ac_id, balance, row_number() over(partition by role_id,ac_id order by created_at desc) rn
						from log_activity_score
						where created_at < 1635206400
						and ac_id in ( '2010701')
						)
				where rn = 1
				) s on u.uuid = s.role_id
	) as t0
	left join (SELECT payments.role_id, sum(price) sum_price
						    FROM payments
							WHERE
								payments.is_paid = 1 
								AND payments.STATUS = 2 
								AND payments.is_test = 2 
								and created_at >= 1635120000 and created_at < 1635206400 --活动期间付费
							GROUP BY
								payments.role_id 

								) pay on t0.uuid = pay.role_id) t
group by ac_id,type,server_id


union all

select  ac_id,type,server_id,count(distinct uuid) uv, count(distinct role_id) paid_users, count(distinct role_id)/count(distinct uuid) pr
from
( select uuid,server_id,ac_id,type,role_id,sum_price
	from(
	select u.uuid,server_id,ac_id,type
	from (
		select uuid, server_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') reg_date
	from roles
	where server_id in ('S15', 'S16') and is_internal = false
	and created_at >= 1634774400 and created_at < 1635206400
	) u 
	inner join (select role_id, ac_id,
				  	   case when balance between 0 and 2000000 then 'p4'
			  	   when balance between 2000001 and 4000000 then 'p6'
			  	   when balance between 4000001 and 6700000 then 'p8'
			  	   when balance between 6700001 and 8000000 then 'p9'
			  	   when balance between 8000001 and 10000000 then 'p10' 
			  	   when balance > 10000000 then 'p11' end type
				from (select role_id,ac_id, balance, row_number() over(partition by role_id,ac_id order by created_at desc) rn
						from log_activity_score
						where created_at < 1635206400
						and ac_id in ('2010601')
						)
				where rn = 1
				) s on u.uuid = s.role_id
	) as t0
	left join (SELECT payments.role_id, sum(price) sum_price
						    FROM payments
							WHERE
								payments.is_paid = 1 
								AND payments.STATUS = 2 
								AND payments.is_test = 2 
								and created_at >= 1635120000 and created_at < 1635206400 --活动期间付费
							GROUP BY
								payments.role_id 

								) pay on t0.uuid = pay.role_id) t
group by ac_id,type,server_id

