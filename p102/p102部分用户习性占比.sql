/*
需求：部分用户习性占比
需求目的：对比两类用户是否有相似性
筛选条件：10.4-10.10期间，16级+孵化野怪次数为0的活跃用户（type1），参与地下洞穴的活跃用户（type2）
日期，type1人数（16级以上），type1习性占比，type2人数，type2习性占比
*/





with users as 
(
select login.role_id, login.login_date
from (select distinct role_id, date_format(from_unixtime(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date 
				from log_login_role
				where created_at >= 1633305600 and created_at < 1633910400 
					and year = 2021 and month = 10) login
inner join (select uuid from roles where server_id != '1' and is_internal = false ) u on login.role_id = u.uuid
),


type1 as
(
select u.login_date, count(distinct u.role_id) type1_uv
 from users u 
 inner join (select distinct role_id, base_level, date_format(from_unixtime(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') logout_date 
							from log_logout
							where created_at >= 1633305600 and created_at < 1633910400 
							  and year = 2021 and month = 10
								and base_level >= 16) logout on u.role_id = logout.role_id and u.login_date = logout.logout_date
where not exists (select 1
							from log_hero h
					   where action = '4600'
						 and created_at >= 1633305600 and created_at < 1633910400
						 and u.role_id = h.role_id)
group by u.login_date
),

type2 as
(
select u.login_date, count(distinct u.role_id) type2_uv
from users u 
inner join (select role_id, join_date
							from (select distinct role_id, date_format(from_unixtime(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') join_date
											from log_puzzle
									   where created_at >= 1633305600 and created_at < 1633910400) tmp
							group by role_id, join_date) p on u.role_id = p.role_id and u.login_date = p.join_date
group by u.login_date
),

-- 孵化野怪数为0用户 
type1_habit as 
(
select u.login_date, h.habit, count(distinct u.role_id) type1_h_uv
from users u 
inner join (select distinct role_id, base_level, date_format(from_unixtime(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') logout_date 
							from log_logout
							where created_at >= 1633305600 and created_at < 1633910400 
							  and year = 2021 and month = 10
								and base_level >= 16) logout on u.role_id = logout.role_id and u.login_date = logout.logout_date
inner join (select role_id, habit
						  from (select role_id, habit, row_number() over(partition by role_id order by created_at desc) rank
											from log_habit) a
						 where a.rank = 1) h on u.role_id = h.role_id
where not exists (select 1
										from log_hero h
									 where action = '4600'
									   and created_at >= 1633305600 and created_at < 1633910400
									   and u.role_id = h.role_id)
group by u.login_date, h.habit
),

-- 参与地下洞穴的活跃用户
type2_habit as 
(select u.login_date, h.habit, count(distinct u.role_id) type2_h_uv
from users u 
inner join (select role_id, habit
						  from (select role_id, habit, row_number() over(partition by role_id order by created_at desc) rank
											from log_habit) a
						 where a.rank = 1) h on u.role_id = h.role_id
inner join (select role_id, join_date
							from (select distinct role_id, date_format(from_unixtime(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') join_date
											from log_puzzle
										where created_at >= 1633305600 and created_at < 1633910400) tmp
							group by role_id, join_date) p on u.role_id = p.role_id and u.login_date = p.join_date
group by u.login_date, h.habit
)

select t1.login_date, t1.type1_uv, th1.habit, th1.type1_h_uv, t2.type2_uv, th2.habit, th2.type2_h_uv
from type1 t1 
left join type1_habit th1 on t1.login_date = th1.login_date
left join type2 t2 on t1.login_date = t2.login_date
left join type2_habit th2 on t1.login_date = th2.login_date and th1.habit = th2.habit;