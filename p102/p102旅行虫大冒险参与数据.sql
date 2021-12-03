需求：旅行虫大冒险参与数据
需求目的：分析活动效果
筛选条件：第一次活动9.14-9.20，第二次活动9.28-10.4
数据结构：
日期，DAU，去新DAU，参与玩家数，参与次数


select log_day,count(distinct t0.role_id) as dau,
		count(distinct if (log_day!=reg_day,t0.role_id,null)) as "去新dau",
		count(distinct t1.role_id) as "参与玩家数",
		sum(pv) as "参与次数"
from
(
	select role_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as log_day,
		DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as reg_day
	from log_login_role inner join roles on roles.uuid=log_login_role.role_id
	where log_login_role.created_at >= 1631577600 --9.14
	and is_internal=false and roles.server_id!='1'
	
  group by role_id,
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ),
		DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )


)as t0
left join 
(
select role_id, count(role_id)as pv,
	DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as act_day
from log_activity_score
where created_at >= 1631577600 --9.14
and action='9014' and ac_id='1000122'
and role_id in (select uuid from roles where  is_internal=false and server_id!='1')
  group by role_id, 
	DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t1
on t1.role_id=t0.role_id and log_day=act_day
group by log_day



select score_range,count(distinct role_id) as uv
from
(
select role_id,case when balance between 1 and 100 then '1-100'
 				when balance between 101 and 200 then '101-200'
  				when balance between 201 and 300 then '201-300'
  				when balance between 301 and 400 then '301-400'
    			when balance between 401 and 500 then '401-500'
     			when balance between 501 and 600 then '501-600'
     			when balance between 601 and 700 then '601-700'
     			when balance between 701 and 800 then '701-800'
     			else 'qita' end as score_range

from
(
select role_id,balance, row_number() over(partition by role_id order by created_at desc) as rank
from log_activity_score
--where created_at >= 1631577600 --9.14
--and created_at < 1632182400

where created_at >=1632787200 and created_at < 1633392000
and action='9014'and ac_id='1000122'
and role_id in (select uuid from roles where  is_internal=false and server_id!='1')
)as t1 where rank=1
) as t2 group by score_range
