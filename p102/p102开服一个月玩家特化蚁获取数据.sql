提数需求
1.需求：开服一个月玩家特化蚁获取数据
2.需求目的：开服一个月玩家特化蚁获取情况
3.筛选条件
只取服务器开服30天时的数据，服务器：220-233，377-388 
玩家筛选条件：玩家注册20天后登录过且登录天数大于14天
4.数据结构
服务器 玩家id 玩家创建日期 最后登录时间 历史充值金额 登录天数 橙色特化蚁获取数量


with 
server as
( select uuid,DATE_FORMAT(FROM_UNIXTIME(open_at) AT TIME ZONE '+00:00', '%Y-%m-%d') open_date
  from servers 
  where (try_cast(uuid as int) between 220 and 233
  	or (try_cast(server_id as int) between 337 and 388)
)),


users as (
select uuid,server_id,reg_date
	from
	(select u.uuid, server_id, DATE_FORMAT(FROM_UNIXTIME(u.created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') reg_date,
	  open_date
	
	from roles u 
	inner join server  on u.server_id = server.uuid
	where (try_cast(server_id as int) between 220 and 233
	or try_cast(server_id as int) between 337 and 388) and is_internal = false
	)as t1
	inner join 
	( select distinct role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
		from log_login_role
	)as t2 
	on t1.uuid=t2.role_id
	where DATE_DIFF ( 'day', DATE_PARSE ( open_date, '%Y-%m-%d' ), DATE_PARSE (log_date, '%Y-%m-%d' ) ) >=20
)

select t1.*,last_login_date,pay_his
from
(
select u.uuid,u.server_id,open_date,reg_date,
 count(distinct login_date) login_days,
 count(distinct hero_id) hero_cnt
from users u
inner join
(select distinct role_id, server_id,open_date,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date
			from log_login_role inner join server on server.uuid=server_id
			where  (try_cast(server_id as int) between 220 and 233
or try_cast(server_id as int) between 337 and 388)
and  DATE_DIFF ( 'day', DATE_PARSE ( open_date, '%Y-%m-%d' ), 
					DATE_PARSE (DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d'), '%Y-%m-%d' ) ) <=29		
) login on u.uuid = login.role_id
left join 
(select distinct role_id, server_id,hero_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') get_date
 from log_hero h
 inner join server on server.uuid=h.server_id
				where (try_cast(h.server_id as int) between 220 and 233
					or try_cast(h.server_id as int) between 337 and 388)
				and hero_id in (1000122,1000123,1000124,1000125,1000126,1000127,1000158,1000159,1000160,1000161,1000162,
								1000163,1000164,1000165,1000166,1000167,1000168,1000169,1000170,1000171,1000172,1000173,
								1000174,1000175,1000176,1000177,1000178,1000179,1000180)
				and hero_type = 4
				and action in ('1400', '3300', '4100', '4102', '5501', '5502', '5503', '5504', '5505', '5506', '5507',
					'5509','7000','9042','9043')
				and  DATE_DIFF ( 'day', DATE_PARSE ( open_date, '%Y-%m-%d' ), 
					DATE_PARSE (DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d'), '%Y-%m-%d' ) ) <=29
		) hero on u.uuid = hero.role_id
group by uuid,u.server_id,open_date,reg_date
) as t1
left join 
(
 select role_id,server_id,sum(price) as pay_his
 from payments inner join server on server.uuid=server_id
 where is_test=2
 and is_paid=1
 and status=2
 and  DATE_DIFF ( 'day', DATE_PARSE ( open_date, '%Y-%m-%d' ), 
					DATE_PARSE (DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d'), '%Y-%m-%d' ) ) <=29
 and DATE_DIFF ( 'day', DATE_PARSE ( open_date, '%Y-%m-%d' ), 
					DATE_PARSE (DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d'), '%Y-%m-%d' ) ) >=0
 group by role_id,server_id
) as pay on t1.uuid = pay.role_id
left join (
select  role_id, server_id,open_date,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') last_login_date,
     row_number() over(partition by role_id order by created_at desc) as rank
			from log_login_role inner join server on server.uuid=server_id
			where  (try_cast(server_id as int) between 220 and 233
or try_cast(server_id as int) between 337 and 388)
and  DATE_DIFF ( 'day', DATE_PARSE ( open_date, '%Y-%m-%d' ), 
					DATE_PARSE (DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d'), '%Y-%m-%d' ) ) <=29	

) as t3 on t3.role_id=t1.uuid
where login_days>=14 and t3.rank=1