11-100服，开服至今，每7天汇总一次持有不同数量橙色特化蚁的玩家数，数据格式如下
服务器，dau,开服第7天，拥有1只橙色蚁的人数，2，3，4，第14天，第21天
select distinct action 
from log_hero
where action in ('1212','1511','2800','2801','2802','2900','2901','2902','2092','2093','4100','5501','5503','5505','5513','5514','5515','5516','5517','5518','5519','5520','5521','6004','5502','5504','5506')





with users as
(
select uuid,server_id,country,paid,
	DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
	,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where is_internal=false and server_id!='1'
   --  and try_cast(server_id as int) >= 11
 ) ,
mission as 
(
	select tmp1.role_id,server_id,tmp1.hero_id,hero_type,mis_day
	from 
	(
	select  mis_day,server_id,
			split(t2.heroid,',')[1] as role_id,
			split(t2.heroid,',')[2] as hero_id
	from
		(select role_id,server_id,mis_day,open_day,
			date_diff(
				'day',
				date_parse ( open_day, '%Y-%m-%d' ),
				date_parse ( mis_day, '%Y-%m-%d' ) ) as diff
				
				,json_extract(army_id,'$.armymsg[0].heroid')  as hero1
				,json_extract(army_id,'$.armymsg[1].heroid')  as hero2
				,json_extract(army_id,'$.armymsg[2].heroid')  as hero3

		from
		(
		 select role_id,server_id,
		 	DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day
		 	,army_id
		 		
		 from log_missions 
		where try_cast(server_id as int) between 11 and 100
		 and is_return=0

		 
		) as t1 
		inner join (
			select uuid,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d') as open_day
			from servers
			where try_cast(uuid as int) between 11 and 100
		) sers on sers.uuid=t1 .server_id 
		where date_diff(
				'day',
				date_parse ( open_day, '%Y-%m-%d' ),
				date_parse ( mis_day, '%Y-%m-%d' ) ) in (6,13,20,27,34,41,48,55,62,69,76,83,90,97,104,111,118,125,132,139,146,153,160,167,174,181)
		) 
		CROSS JOIN UNNEST(array[concat(cast(role_id as varchar),',',cast(hero1 as varchar)),
		concat(cast(role_id as varchar),',',cast(hero2 as varchar)),
		concat(cast(role_id as varchar),',',cast(hero3 as varchar))]) AS t2 (heroid)

		group by mis_day,server_id,split(t2.heroid,',')[1] ,split(t2.heroid,',')[2] 
		having split(t2.heroid,',')[2]  is not null and split(t2.heroid,',')[2] !='0'
	)as tmp1
	inner join(
		select hero_id,hero_type
		from log_hero
		where hero_type=4
		 	and try_cast(server_id as int) between 11 and 100

		 group by hero_id,hero_type
	) as tmp2 on cast(tmp1.hero_id as int)=tmp2.hero_id
	 where tmp1.hero_id is not null and tmp1.hero_id!='0'
	 	group by tmp1.role_id,server_id,tmp1.hero_id,hero_type,mis_day
),
skill as (
	select role_id,server_id,hero_id,hero_type,skill_day
	from(
	select role_id,server_id,hero_id,hero_type,skill_id,skill_level ,
			DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as skill_day
	from log_hero_skill
	where action in ('2900','2901','2902')--英雄解锁、升级
      and  hero_type=4
		 and try_cast(server_id as int) between 11 and 100
	) as tmp
	group by role_id,server_id,hero_id,hero_type,skill_day
	
),

hero as (
select role_id,server_id,hero_id,hero_type,
DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as hero_day
from log_hero

where  action in ('1212','1511','2800','2801','2802','2900','2901','2902','2092',
	'2093','4100','5501','5503','5505','5513','5514','5515','5516','5517','5518',
	'5519','5520','5521','6004','5502','5504','5506')
     and hero_type=4
  	and try_cast(server_id as int) between 11 and 100
		group by role_id,server_id,hero_id,hero_type,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 

  )


select *
from
(

select b.server_id,dau,open_day,act_day,hero_type,nums,uv,
	date_diff(
				'day',
				date_parse ( open_day, '%Y-%m-%d' ),
				date_parse ( act_day, '%Y-%m-%d' ) ) as diff
from

(
select server_id,act_day,hero_type,nums,count(distinct role_id) as uv
from
(
select role_id,
	count( distinct hero_id) as nums,
	hero_type,
	act_day,server_id
from
(
select act.*
from
(
select role_id,server_id,hero_id,hero_type,hero_day as act_day
  from	hero  
  
	union
  select role_id,server_id,hero_id,hero_type,skill_day as act_day
  from	skill
  
	union
  select role_id,server_id,cast(hero_id as int) hero_id,hero_type,mis_day as act_day
  from mission
  
)as act
inner join users on users.uuid=act.role_id
where try_cast(act.server_id as int) between 11 and 100
) as t1 group by role_id,
	hero_type,
	act_day,server_id
)as t2 group by server_id,act_day,hero_type,nums
) a

inner join (
	select count(distinct role_id) as dau,server_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d') as log_day
	from log_login_role 
	where role_id in (select uuid from roles where is_internal=false and server_id!='1')
			and try_cast(server_id as int) between 11 and 100
	group by server_id, DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d')

) b on a.server_id=b.server_id and a.act_day=b.log_day
inner join (
	select uuid,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d') as open_day
	from servers
	where try_cast(uuid as int) between 11 and 100
) c on c.uuid=a.server_id 
 ) as tmp 
where diff in (6,13,20,27,34,41,48,55,62,69,76,83,90,97,104,111,118,125,132,139,146,153,160,167,174,181)

