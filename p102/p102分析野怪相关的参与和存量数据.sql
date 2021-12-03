需求：野怪相关数据
需求目的：分析野怪相关的参与和存量数据
筛选条件1：大于等于16级，最近一周活跃的玩家，按设备ID
数据结构1（每日孵化野怪个数）：action=4600
日期（9.20-9.26），孵化X个，玩家数




with major_users as
(
    select uuid,server_id,reg_day,base_level,last_login_day,paid
    from
(
select uuid,paid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day 
        ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles 
where base_level>=16 and last_login_at>=1632182400
 ) as a
inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,
            row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
        
    ) b on a.uuid=b.role_id
   

)
-- 每日孵化野怪个数

select coalesce(nums,0) as nums,log_day,count(distinct uuid) as uv
from
(
	select uuid,log_day,act_day,nums
	from major_users
	left join
	(select distinct role_id ,
            DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as log_day
        from log_login_role
        where  created_at>=1632096000
        and year=2021 and month>=9
	)
	as t0 on t0.role_id=major_users.uuid
	left join
	( select role_id,count(role_id) as nums,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as act_day
		from log_hero
		where action in ('4600') and created_at>=1632096000 and year=2021 and month>=9
		group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
	)as t1
	on t1.role_id=t0.role_id and t1.act_day=t0.log_day
) as t2
group by  coalesce(nums,0),log_day





