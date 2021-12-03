行军参与情况
筛选条件：时间8.1-提数前一天，按设备ID，等级大于等于8级
表结构1：日期，行军类型，等级，>=8级所有活跃玩家数，参与玩家数，行军次数
表结构2：日期，行军类型，等级，>=8级且注册时间一周以内的活跃玩家数，参与玩家数，行军次数
表结构3：日期，行军类型，等级，>=8级且注册时间一周以上的活跃玩家数，参与玩家数，行军次数

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
where created_at>=1627776000 and created_at<1630281600
		and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
) as t1 where rank=1 --每天第一次登录时等级

)


select t1.log_day,t1.base_level,t2.log_uv,category,mis_uv,nums

from
(
select log_day,category,base_level,
	count(distinct mis_uid) as mis_uv,
	sum(nums)  as nums
	
from
(	
	select  log_day,log_users.base_level,
			log_users.role_id as log_uid,
			mis.role_id as mis_uid,
			major_users.server_id,
			mis_day,category,nums
from log_users
inner join major_users on major_users.uuid=log_users.role_id
left join 
(	select role_id,category,count(role_id) as nums,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day

	from log_missions
	where created_at>=1627776000 and created_at<1630281600
	-- and year=2021 and month>=7
	and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
	group by role_id,category
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
)as mis on log_users.role_id=mis.role_id and log_users.log_day=mis.mis_day
where log_users.base_level>=8
)as tmp 
-- where try_cast(server_id as integer)>=224
group by log_day,category,base_level

)as t1
inner join
(
	select  log_day,log_users.base_level,
			count(distinct role_id ) as log_uv
	from log_users
	inner join major_users on major_users.uuid=log_users.role_id
	-- where try_cast(server_id as integer)>=224
	group by log_day,log_users.base_level
) as t2 on t1.log_day=t2.log_day and t1.base_level=t2.base_level






###2
with log_users as
(
	select role_id,base_level,log_day,reg_day,
			DATE_DIFF ( 'day', DATE_PARSE ( reg_day, '%Y-%m-%d' ), 
					DATE_PARSE (log_day, '%Y-%m-%d' ) ) AS diff
	from(
		select role_id,base_level,log_day
		from
		(
		select role_id,base_level,
				DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
				,row_number() over(partition by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
					 order by created_at desc) as rank
		from log_logout
		where created_at>=1627776000 and created_at<1630281600
			and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
		) as t1 where rank=1 --每天第一次登录时等级
		)as log inner join 
		(	select uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day
			from roles

		) as reg on reg.uuid=log.role_id
		where DATE_DIFF ( 'day', DATE_PARSE ( reg_day, '%Y-%m-%d' ), 
					DATE_PARSE (log_day, '%Y-%m-%d' ) )<=6
		

)

select t1.log_day,t1.base_level,t2.log_uv,category,mis_uv,nums

from
(
select log_day,category,base_level,
	count(distinct mis_uid) as mis_uv,
	sum(nums)  as nums
	
from
(	
	select  log_day,log_users.base_level,
			log_users.role_id as log_uid,
		
			mis.role_id as mis_uid,
			mis_day,category,nums
from log_users
inner join major_users on major_users.uuid=log_users.role_id
left join 
(	select role_id,category,count(role_id) as nums,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day

	from log_missions
	where created_at>=1627776000 and created_at<1630281600
	and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
	group by role_id,category
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
)as mis on log_users.role_id=mis.role_id and log_users.log_day=mis.mis_day
where log_users.base_level>=8
)as tmp group by log_day,category,base_level
)as t1
inner join
(
	select  log_day,log_users.base_level,
			count(distinct role_id ) as log_uv
	from log_users
	inner join major_users on major_users.uuid=log_users.role_id
	group by log_day,log_users.base_level
) as t2 on t1.log_day=t2.log_day and t1.base_level=t2.base_level