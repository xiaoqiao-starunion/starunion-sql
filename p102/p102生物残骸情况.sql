p102生物残骸情况.sql
筛选条件：注册时间大于等于1个月，等级大于等于8级，取设备ID下最大等级角色
用户分层：8.30-9.05 活跃的
type1：零充用户，在线时长0-30分钟(活跃每日平均在线时长 总在线时长/活跃天数)
type2：零充用户，在线时长30-90分钟
type3：零充用户，在线时长90分钟以上
type4：充值金额10美金以内，在线时长0-30分钟
type5：充值金额10美金以内，在线时长30-120分钟
type6：充值金额10美金以内，在线时长120分钟以上
type7：充值金额10-100美金，在线时长0-30分钟
type8：充值金额10-100美金，在线时长30-120分钟
type9：充值金额10-100美金，在线时长120分钟以上
type10：充值金额100-1000美金，在线时长0-120分钟
type12：充值金额100-1000美金，在线时长120+分钟以上
type13：充值金额1000美金以上，在线时长0-120分钟
type14：充值金额1000美金以上，在线时长120+分钟
表结构：
日期（8.30-9.5），用户类型（type1-15），用户数量，人均在线时长，在线时长中位数，
生物残骸获取途径，获取人数，获取数量，生物残骸获取中位数（该类型玩家在当日该获取途径中生物残骸的中值）


with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day,paid
        from
(
select uuid,server_id,paid
        ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where created_at < 1628553600 --注册时间<=8.9

    and base_level>=8
    and is_internal=false and server_id!='1'

 ) as a

inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        
        ) temp1 where rank=1
        
    ) b on a.uuid=b.role_id

inner join 
( select distinct role_id
	from log_login_role
	where created_at >= 1630281600 and created_at<1630886400 --8.30-9.5活跃的用户
) as c on a.uuid=c.role_id
),


user_type as 
(
--select count(distinct role_id) as uv,type
--from
--(
select role_id,reg_day,paid,online_time/online_days as online_time_per_day
		, case 
		  when paid=0 and (online_time/online_days) between 0 and 30 then 'type1'
		  when paid=0 and (online_time/online_days) between 30 and 90 then 'type2'
		  when paid=0 and (online_time/online_days)   >90 then 'type3'
		  when paid>0 and paid<10 and (online_time/online_days) between 0 and 30 then 'type4'
		  when paid>0 and paid<10 and (online_time/online_days) between 30 and 120 then 'type5'
		  when paid>0 and paid<10 and (online_time/online_days)   >120 then 'type6'
		  when paid>=10 and paid<100 and (online_time/online_days) between 0 and 30 then 'type7'
		  when paid>=10 and paid<100 and (online_time/online_days) between 30 and 120 then 'type8'
		  when paid>=10 and paid<100 and (online_time/online_days)   >120 then 'type9'
		  when paid>=100 and paid<1000 and (online_time/online_days) between 0 and 120 then 'type10'
		  when paid>=100 and paid<1000 and (online_time/online_days)   >120 then 'type11'
		  when paid>=1000  and (online_time/online_days) between 0 and 120 then 'type12'
		  when paid>=1000  and (online_time/online_days)   >120 then 'type13'
		  else '其他' end as type

from(
	select role_id,sum(secs)/60 as online_time
			,count(distinct DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )) as online_days
	from log_session
	group by role_id

) as online inner join major_users on major_users.uuid=online.role_id
--) as user_base group by type
)


select user_type.role_id,type,online_time_per_day,action_day,action,get_nums_all

from user_type 
left join (
select role_id,action_day,action,sum(get_nums) as get_nums_all
from (
		select role_id,action_day,action,
				sum(nums) as get_nums
		from
		(
		SELECT role_id,item_id,nums,action,
				DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
		FROM log_item 
		where item_id = 204066--生物残骸
		  and nums>0 --获取
		 and month>=8
		 and created_at >= 1630281600 and created_at<1630886400 --8.30-9.5

		) as item group by role_id,action_day,action

   
) as tmp group by role_id,action_day,action

) as get on get.role_id=user_type.role_id