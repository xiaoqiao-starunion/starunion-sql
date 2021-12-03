提数需求
条件：截止到8月31号23点59分59秒的在每个服拥有特级异化卵数量大于等于100的玩家
表结构：玩家id,玩家昵称，玩家所在服务器，玩家拥有特级异化卵数量
@动人  


select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
	and month>=8
	) as t where rank=1 and balance>=100
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id


特级异化卵-存量
提数需求
条件：截止到8月31号23点59分59秒的在每个服拥有特级异化卵数量大于等于100的玩家
表结构：玩家id,玩家昵称，玩家所在服务器，玩家拥有特级异化卵数量
@动人  


select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
	and month>=8
	) as t where rank=1 and balance>=100
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id






1、9月13日7点以前2~368服玩家特级异化卵余量
2、9月13日23:59分2~368服玩家特级异化卵余量
3、9月13日0点到7点玩家消耗特级异化卵情况
4、9月13日9点到23:59分玩家消耗特级异化卵情况
5、9月13日23:59分玩家特级异化卵余量
表结构：玩家的id，昵称，服务器，特级异化卵数量/消耗数量
以上时间为utc时间 @小乔 

1、7点和晚上24点的余量服务器，dau,服务器，总的存量
2、7点和晚上24点余量在巴拉巴拉个以上的玩家：服务器，玩家id，玩家昵称，玩家特级异化卵持有量
3、0-7点前和9点后-24点玩家消耗情况：服务器，dau，变化数量
4、0-7点前和9点后-24点玩家消耗情况：服务器，玩家id，玩家昵称，玩家消耗特级异化卵数量


select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
	and created_at <=1631516400 -- 9月13日7点以前
	--and created_at <=1631577600 -- 9月13日24点以前
	and try_cast(server_id as integer) between 2 and 368
	
	) as t where rank=1 and balance>10
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id




select role_id,server_id,name,paid,nums
from
(	select role_id, nums
	from
	(
	SELECT role_id, sum(nums) as nums
	FROM log_item 
	where item_id=204023 and nums<0
	and created_at>=1631491200 and created_at <1631516400 -- 9月13日0-7：00
	-- and created_at>=1631523600 and created_at <1631577600 -- 9月13日9点-24点
	and try_cast(server_id as integer) between 2 and 368
	
	group by role_id
	) as t 
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id





select t1.server_id,dau,uv,balance_total
from
(select server_id,count(distinct role_id) as uv,sum(balance) as balance_total
from
(
select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
	and created_at <=1631516400 -- 9月13日7点以前
	--and created_at <=1631577600 -- 9月13日24点以前
	and try_cast(server_id as integer) between 2 and 368

	) as t where rank=1 and balance>10
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id
) as tmp group by server_id
) as t1
inner join
(
select count(distinct role_id) as dau,server_id,
	DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day
from log_login_role
where role_id in (select uuid
	from roles
	where is_internal=false and server_id!='1')
and try_cast(server_id as integer) between 2 and 368
group by server_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
) as t2
on t1.server_id=t2.server_id and log_day='2021-09-13'


--消耗汇总

--消耗
select t1.server_id,dau,uv,nums_total,act_day
from
(
select server_id,count(distinct role_id) as uv,sum(nums) as nums_total,act_day
from
(select role_id,server_id,name,paid,nums,act_day
from
(	select role_id, nums,act_day
	from
	(
	SELECT role_id, sum(nums) as nums,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as act_day
	FROM log_item 
	where item_id=204023 
	and nums<0 --消耗
	and created_at>=1631491200 and created_at <1631750400 -- 时间范围：9月13日-15日
	and month>=9
	group by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
	) as t 
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id
)as tmp group by server_id ,act_day
) as t1
inner join 
(select count(distinct role_id) as dau,server_id,
	DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day
from log_login_role
where role_id in (select uuid
	from roles
	where is_internal=false and server_id!='1')
	
group by server_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
) as t2
on t1.server_id=t2.server_id and act_day=log_day







--余量

先取最后的时间，再取时间对应的日志。加分区

select '2021-09-13' as date,t1.server_id,uv,balance_total
from
(select server_id,count(distinct role_id) as uv,sum(balance) as balance_total
from
(
select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
	
	and created_at <=1631491200 -- 9月13日0点以前
	and year=2021 and month in (3,4,5,6,7,8,9)

	) as t where rank=1 
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id
) as tmp group by server_id
) as t1


union


select '2021-09-14' as date,t1.server_id,uv,balance_total
from
(select server_id,count(distinct role_id) as uv,sum(balance) as balance_total
from
(
select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
	and year=2021 and month in (3,4,5,6,7,8,9)
	and created_at <=1631577600 -- 9月14日0点以前
	

	) as t where rank=1 
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id
) as tmp group by server_id
) as t1
union
select '2021-09-15' as date,t1.server_id,uv,balance_total
from
(select server_id,count(distinct role_id) as uv,sum(balance) as balance_total
from
(
select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
	and year=2021 and month in (3,4,5,6,7,8,9)
	and created_at <=1631664000 -- 9月15日0点以前
	

	) as t where rank=1 
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id
) as tmp group by server_id
) as t1
union
select '2021-09-16' as date,t1.server_id,uv,balance_total
from
(select server_id,count(distinct role_id) as uv,sum(balance) as balance_total
from
(
select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204023
    and year=2021 and month in (3,4,5,6,7,8,9)
	and created_at <=1631750400 -- 9月16日0点以前
	

	) as t where rank=1 
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id
) as tmp group by server_id
) as t1