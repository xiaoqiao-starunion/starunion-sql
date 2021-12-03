需求：行军参与情况
需求目的：分析参与情况变化
筛选条件：时间：7.1-10.17，等级大于等于8级，唯一设备ID
数据结构
日期，等级，去新DAU，行军类型，去新参与玩家数，行军次数
 @小乔 

-- 大号：
with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day
        from
(
select uuid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where 
     is_internal=false and server_id!='1'
  
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

),


 log_users as
(select role_id,base_level,log_day
from
(
select role_id,base_level,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
		,row_number() over(partition by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
			 order by created_at desc) as rank
from log_logout 
where created_at>=1625097600 and created_at<1634515200
		and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
) as t1 where rank=1 --每天第一次登录时等级
       and base_level>=8
)


select t1.log_day,t1.history_level,t2.log_uv,category,mis_uv,nums

from
(
select log_day,category,history_level,
	count(distinct mis_uid) as mis_uv,
	sum(nums)  as nums
	
from
(	
	select  log_day,users.history_level,
			mis.role_id as mis_uid,
			users.server_id,
			mis_day,category,nums
from
(select uuid,server_id,reg_day,major_users.base_level,last_login_day,log_users.base_level as history_level,log_day
from  major_users
inner join log_users on major_users.uuid=log_users.role_id
where  reg_day!=log_day
)as users
left join 
(	select role_id,category,count(role_id) as nums,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day

	from log_missions
	where created_at>=1625097600 and created_at<1634515200
	and is_return=0
	 and year=2021 and month>=7
	and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
	group by role_id,category
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
)as mis on users.uuid=mis.role_id and users.log_day=mis.mis_day
        
)as tmp 
-- where try_cast(server_id as integer)>=224
group by log_day,category,history_level

)as t1
inner join
(
	select  log_day,log_users.base_level,
			count(distinct role_id ) as log_uv
	from log_users
	inner join major_users on major_users.uuid=log_users.role_id
	-- where try_cast(server_id as integer)>=224
	where  reg_day!=log_day
	group by log_day,log_users.base_level
) as t2 on t1.log_day=t2.log_day and t1.history_level=t2.base_level
