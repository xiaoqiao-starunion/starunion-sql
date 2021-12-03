特级异化卵产出情况
筛选条件：5月注册用户，等级大于等于8级，取设备ID下最大等级角色
用户分层：
选条件：注册时间大于等于1个月，等级大于等于8级，取设备ID下最大等级角色
用户分层：活跃天数》=20
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
type11：充值金额100-1000美金，在线时长120+分钟以上
type12：充值金额1000美金以上，在线时长0-120分钟
type13：充值金额1000美金以上，在线时长120+分钟
表结构：
月份（5-8月），用户类型（type1-13），用户数量，人均在线时长，在线时长中位数，特级异化卵获取途径，获取人数，获取数量，特级异化卵获取中位数（该类型玩家在当日该获取途径中特级异化卵的中值）
以上条件再分别取6，7，8月注册用户分别在后续月份的获取情况



with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day,paid
        from
(
select uuid,server_id,paid
        ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where created_at>=1619827200 and created_at < 1622505600 --注册时间5月份
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
( select  role_id,count(distinct DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) )as act_days--活跃天数
	from log_login_role
	group by role_id
) as c on a.uuid=c.role_id
where act_days>=20
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


select user_type.role_id ,type,online_time_per_day,action_month,action,get_nums_all
from user_type 
left join ( --确认一下是要表标签人数还是期间标签活跃人数
select role_id,action_month,action,sum(get_nums) as get_nums_all
from (
		select role_id,action_month,action,
				sum(nums) as get_nums
	    from
	   (	
		SELECT role_id,item_id,nums,action,
				DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m') as action_month
		FROM log_item 
		where item_id = 204023 --特级异化卵
		  and nums>0 --获取
		 and month>=5
		 and created_at >= 1619827200 and created_at<1630454400 --5.1-8.30
		
		) as item group by role_id,action_month,action

) as tmp group by role_id,action_month,action

) as get on get.role_id=user_type.role_id


