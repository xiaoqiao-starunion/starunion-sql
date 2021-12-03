p102集结参与情况
筛选条件：服务器243-245
表结构：
服务器，开服第1天-30天，DAU，集结参与人数，参与次数，集结对象1次数，集结对象2次数。。。集结对象X次数 @小乔 
集结对象只看野怪的，如果太多就只取前10个



select t1.server_id,t1.log_day,t1.diff,t3.log_uv,
		t1.category,mis_uv_all,mis_pv_all,battle_id,mis_uv,mis_pv

from
(
select d1.server_id,d1.diff,
	d1.category,
	d1.battle_id,
	log_day,
	count(distinct mis_uid) as mis_uv,
	sum(nums)  as mis_pv
	

	
from
(	
	select  log_users.log_day,server_id,
			date_diff('day', DATE_PARSE ( open_day, '%Y-%m-%d' ), DATE_PARSE (log_day, '%Y-%m-%d' ))as diff,
			log_users.role_id as log_uid,
			mis.role_id as mis_uid,
			mis_day,category,battle_id,nums
	from 
	(   select role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
		from log_login_role
		where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
		group by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
	) as log_users
	left join
	(	select role_id,server_id,category,battle_id,count(role_id) as nums,
			DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day

		from log_missions
		where category = 7 --集结
			and try_cast(battle_id as integer) between 9400 and 22160 --野怪id
			and is_return=0
			and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
			and year=2021 and month>=7
		group by role_id,category,server_id,battle_id
			,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
	)as mis on log_users.role_id=mis.role_id and log_users.log_day=mis.mis_day
	inner join(
		select uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as open_day
		from servers
		where try_cast(uuid as integer) between 243 and 245
	)as servers on servers.uuid=mis.server_id

) as d1
-- cross join unnest(array(if(battle_id <> '', battle_id, '_null'), "_NONE")) d2 as battle_id  有问题 
group by d1.diff,d1.category,d1.battle_id,d1.server_id,log_day


)as t1
inner join
(
select d1.server_id,d1.diff,
	d1.category,
	log_day,
	count(distinct mis_uid) as mis_uv_all,
	sum(nums)  as mis_pv_all
	

	
from
(	
	select  log_users.log_day,server_id,
			date_diff('day', DATE_PARSE ( open_day, '%Y-%m-%d' ), DATE_PARSE (log_day, '%Y-%m-%d' ))as diff,
			log_users.role_id as log_uid,
			mis.role_id as mis_uid,
			mis_day,category,battle_id,nums
	from 
	(   select role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
		from log_login_role
		where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
		and year=2021 and month>=7
		group by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
	) as log_users
	
	left join
	(	select role_id,server_id,category,battle_id,count(role_id) as nums,
			DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day

		from log_missions
		where category = 7 -- 集结
			and try_cast(battle_id as integer) between 9400 and 22160 --野怪id
			and is_return=0
			and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
			and year=2021 and month>=7
		group by role_id,category,server_id,battle_id
			,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
	)as mis on log_users.role_id=mis.role_id and log_users.log_day=mis.mis_day
	inner join(
		select uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as open_day
		from servers
		where try_cast(uuid as integer) between 243 and 245
	)as servers on servers.uuid=mis.server_id

) as d1
group by d1.diff,d1.category,d1.server_id,log_day


) t2 on t1.log_day=t2.log_day and t1.server_id=t2.server_id

inner join
(
	select  DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) log_day,server_id,
			count(distinct role_id ) as log_uv
	from log_login_role
	where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
	and year=2021 and month>=7
	group by DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) ,server_id
) as t3 on t1.log_day=t3.log_day and t1.server_id=t3.server_id

where t1.diff<=30




集结参与情况：
筛选条件：185-189，264-269，296-299
服务器，开服第1天-60天，DAU，集结参与人数，参与次数，集结对象1次数，集结对象2次数。。。集结对象X次数
能不能在多筛选几个区服，看1-60天的