p102特级异化卵累计获取数量
条件：11~14服分别在开服7天、14天、21天、28天后玩家累计拥有的特级异化卵数量在1~30,31~45,46~60,61~75,76~90,91~105，105以上范围的人数（具体日期和之前日期一样）
表结构：时间，服务器，累计特级异化卵数量各范围 @小乔 

select date,server_id,
		case when total_nums<=30 and total_nums>=1 then '1~30'
		  when total_nums<=45 and total_nums>=31 then '31~45'
		  when total_nums<=60 and total_nums>=46 then '46~60'
		  when total_nums<=75 and total_nums>=61 then '61~75'
		  when total_nums<=90 and total_nums>=76 then '76~90'
		  when total_nums<=105 and total_nums>=91 then '91~105'
		  when total_nums>=105  then '105+'
		  end as nums_range,
     count(distinct role_id) as uv

from
(
select role_id,server_id,sum(nums) as total_nums,'开服7天' as date
from
(	select role_id,server_id,nums,action_day,open_day,
			DATE_DIFF ( 'day', DATE_PARSE ( open_day, '%Y-%m-%d' ), DATE_PARSE (action_day, '%Y-%m-%d' ) ) AS diff
	from
	(
	SELECT role_id, sum(nums) as nums,
			
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
			and nums>0
	group by role_id, 
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
	) as t  
	inner join 
	( select uuid,server_id
	from roles
	where is_internal=false and server_id!='1'

   )as roles
	on roles.uuid=t.role_id
	inner join (
		SELECT uuid,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as open_day
		FROM servers
		where uuid in ('11','12','13','14')
		)as servers on servers.uuid=roles.server_id
) as users
where diff<=6
group by role_id,server_id,'开服7天' 

union

select role_id,server_id,sum(nums) as total_nums,'开服14天' as date
from
(	select role_id,server_id,nums,action_day,open_day,
			DATE_DIFF ( 'day', DATE_PARSE ( open_day, '%Y-%m-%d' ), DATE_PARSE (action_day, '%Y-%m-%d' ) ) AS diff
	from
	(
	SELECT role_id, sum(nums) as nums,
			
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
			and nums>0
	group by role_id, 
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
	) as t  
	inner join 
	( select uuid,server_id
	from roles
	where is_internal=false and server_id!='1'

   )as roles
	on roles.uuid=t.role_id
	inner join (
		SELECT uuid,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as open_day
		FROM servers
		where uuid in ('11','12','13','14')
		)as servers on servers.uuid=roles.server_id
) as users
where diff<=13
group by role_id,server_id,'开服14天' 

union

select role_id,server_id,sum(nums) as total_nums,'开服21天' as date
from
(	select role_id,server_id,nums,action_day,open_day,
			DATE_DIFF ( 'day', DATE_PARSE ( open_day, '%Y-%m-%d' ), DATE_PARSE (action_day, '%Y-%m-%d' ) ) AS diff
	from
	(
	SELECT role_id, sum(nums) as nums,
			
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
			and nums>0
	group by role_id, 
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
	) as t  
	inner join 
	( select uuid,server_id
	from roles
	where is_internal=false and server_id!='1'

   )as roles
	on roles.uuid=t.role_id
	inner join (
		SELECT uuid,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as open_day
		FROM servers
		where uuid in ('11','12','13','14')
		)as servers on servers.uuid=roles.server_id
) as users
where diff<=20
group by role_id,server_id,'开服21天' 

union

select role_id,server_id,sum(nums) as total_nums,'开服28天' as date
from
(	select role_id,server_id,nums,action_day,open_day,
			DATE_DIFF ( 'day', DATE_PARSE ( open_day, '%Y-%m-%d' ), DATE_PARSE (action_day, '%Y-%m-%d' ) ) AS diff
	from
	(
	SELECT role_id, sum(nums) as nums,
			
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
			and nums>0
	group by role_id, 
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
	) as t  
	inner join 
	( select uuid,server_id
	from roles
	where is_internal=false and server_id!='1'

   )as roles
	on roles.uuid=t.role_id
	inner join (
		SELECT uuid,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as open_day
		FROM servers
		where uuid in ('11','12','13','14')
		)as servers on servers.uuid=roles.server_id
) as users
where diff<=27
group by role_id,server_id ,'开服28天' 
) as tmp
group by date,server_id,
		case when total_nums<=30 and total_nums>=1 then '1~30'
		  when total_nums<=45 and total_nums>=31 then '31~45'
		  when total_nums<=60 and total_nums>=46 then '46~60'
		  when total_nums<=75 and total_nums>=61 then '61~75'
		  when total_nums<=90 and total_nums>=76 then '76~90'
		  when total_nums<=105 and total_nums>=91 then '91~105'
		  when total_nums>=105  then '105+'
		  end 