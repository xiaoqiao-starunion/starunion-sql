p102蚂蚁训练数量20210914
筛选条件：
注册2个月以上，等级大于等于10级
数据格式：
日期（8.30-9.13），玩家等级，活跃人数，参与孵化兵蚁人数，每天孵化兵蚁的数量，参与异变兵蚁人数，异变数量 @小乔 


with users as
( select uuid,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as reg_day
from roles
where created_at<=1626220800 --7.14号之前注册
	and base_level>=10
	and is_internal=false and server_id!='1'
) 


select action_day,a.base_level,log_uv,uv_1,pv_1,uv_2,pv_2
from
(
select count(distinct if(action in ('6100','10005'),t1.role_id,null)) as uv_1,--孵化人数，
		sum(if(action in ('6100','10005'),nums,0)) as pv_1,
		count(distinct if(action in ('6110','6111'),t1.role_id,null)) as uv_2,--升级人数，
		sum(if(action in ('6110','6111'),nums,0)) as pv_2,
		action_day,t2.base_level

from
(select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
		,action,nums
from log_troops inner join users on users.uuid=log_troops.role_id
where action in ('6100','6110','6111','10005')
		and change_type='1'--活兵
)as t1
inner join (
  select role_id,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day,
  row_number() over(partition by role_id , DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') 
  	order by created_at desc ) as rank
  from log_logout
  where created_at>=1630281600 and created_at<1631577600
)as t2 on t1.role_id=t2.role_id and t2.log_day=action_day
where rank=1
group by action_day,base_level
)as a
inner join (
	select count(distinct role_id) as log_uv,base_level,log_day
	from(
	select role_id,log_logout.base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day,
  row_number() over(partition by role_id , DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') 
  	order by created_at desc ) as rank
  from log_logout inner join users on users.uuid=log_logout.role_id
  where created_at>=1630281600 and created_at<1631577600
  )as tmp where rank=1
	group by base_level,log_day
)as b on a.action_day=b.log_day and a.base_level=b.base_level