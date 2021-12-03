/*
1. 需求：
通过log_login_role和log_out表内筛选玩家 分日，最后一次登录及登出时，玩家的主堡等级
2. 需求目的：
玩家分日的主堡等级分布
3. 筛选条件：
服务器S16  时间：UTC 10.21 00:00~10.31 23:59
服务器S17  时间：UTC 10.26 00:00~10.31 23:59
4数据格式：
base_level, day1, day2, day3, day4, day5, day6, day7
*/


-- S16, 登录
select server_id, base_level, '登录',

sum(case when login_date = '2021-10-21' then uv end) as day1,
sum(case when login_date = '2021-10-22' then uv end) as day2,
sum(case when login_date = '2021-10-23' then uv end) as day3,
sum(case when login_date = '2021-10-24' then uv end) as day4,
sum(case when login_date = '2021-10-25' then uv end) as day5,
sum(case when login_date = '2021-10-26' then uv end) as day6,
sum(case when login_date = '2021-10-27' then uv end) as day7,
sum(case when login_date = '2021-10-28' then uv end) as day8,
sum(case when login_date = '2021-10-29' then uv end) as day9,
sum(case when login_date = '2021-10-30' then uv end) as day10,
sum(case when login_date = '2021-10-31' then uv end) as day11,
sum(case when login_date = '2021-11-01' then uv end) as day12,
sum(case when login_date = '2021-11-02' then uv end) as day13,
sum(case when login_date = '2021-11-03' then uv end) as day14
from (select server_id, base_level, login_date, count(distinct role_id) uv
from (select role_id, server_id, base_level, login_date
		from (select role_id, server_id, base_level, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date,
				row_number() over(partition by role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') order by created_at desc) rn
				from log_login_role
				where created_at >= 1634774400 and created_at < 1635984000
				and server_id = 'S16'
				and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
		where rn = 1) t1
group by server_id, base_level, login_date) t2
group by server_id, base_level,'登录'

union all

-- S16, 登出
select server_id, base_level, '登出',
sum(case when login_date = '2021-10-21' then uv end) as day1,
sum(case when login_date = '2021-10-22' then uv end) as day2,
sum(case when login_date = '2021-10-23' then uv end) as day3,
sum(case when login_date = '2021-10-24' then uv end) as day4,
sum(case when login_date = '2021-10-25' then uv end) as day5,
sum(case when login_date = '2021-10-26' then uv end) as day6,
sum(case when login_date = '2021-10-27' then uv end) as day7,
sum(case when login_date = '2021-10-28' then uv end) as day8,
sum(case when login_date = '2021-10-29' then uv end) as day9,
sum(case when login_date = '2021-10-30' then uv end) as day10,
sum(case when login_date = '2021-10-31' then uv end) as day11,
sum(case when login_date = '2021-11-01' then uv end) as day12,
sum(case when login_date = '2021-11-02' then uv end) as day13,
sum(case when login_date = '2021-11-03' then uv end) as day14
from (select server_id, base_level, login_date, count(distinct role_id) uv
from (select role_id, server_id, base_level, login_date
		from (select role_id, server_id, base_level, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date,
				row_number() over(partition by role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') order by created_at desc) rn
				from log_logout
				where created_at >= 1634774400 and created_at < 1635984000
				and server_id = 'S16'
				and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
		where rn = 1) t1
group by server_id, base_level, login_date) t2
group by server_id, base_level,'登出'



-- S17, 登录
select server_id, base_level, '登录',
sum(case when login_date = '2021-10-26' then uv end) as day1,
sum(case when login_date = '2021-10-27' then uv end) as day2,
sum(case when login_date = '2021-10-28' then uv end) as day3,
sum(case when login_date = '2021-10-29' then uv end) as day4,
sum(case when login_date = '2021-10-30' then uv end) as day5,
sum(case when login_date = '2021-10-31' then uv end) as day6,
sum(case when login_date = '2021-11-01' then uv end) as day7,
sum(case when login_date = '2021-11-02' then uv end) as day8,
sum(case when login_date = '2021-11-03' then uv end) as day9
from (select server_id, base_level, login_date, count(distinct role_id) uv
from (select role_id, server_id, base_level, login_date
		from (select role_id, server_id, base_level, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date,
				row_number() over(partition by role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') order by created_at desc) rn
				from log_login_role
				where created_at >= 1635206400 and created_at < 1635984000
				and server_id = 'S17' 
				and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
		where rn = 1) t1
group by server_id, base_level, login_date) t2
group by server_id, base_level,'登录'

union all
 
-- S17, 登出
select server_id, base_level, '登出',
sum(case when login_date = '2021-10-26' then uv end) as day1,
sum(case when login_date = '2021-10-27' then uv end) as day2,
sum(case when login_date = '2021-10-28' then uv end) as day3,
sum(case when login_date = '2021-10-29' then uv end) as day4,
sum(case when login_date = '2021-10-30' then uv end) as day5,
sum(case when login_date = '2021-10-31' then uv end) as day6,
sum(case when login_date = '2021-11-01' then uv end) as day7,
sum(case when login_date = '2021-11-02' then uv end) as day8,
sum(case when login_date = '2021-11-03' then uv end) as day9
from (select server_id, base_level, login_date, count(distinct role_id) uv
from (select role_id, server_id, base_level, login_date
		from (select role_id, server_id, base_level, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date,
				row_number() over(partition by role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') order by created_at desc) rn
				from log_logout
				where created_at >= 1635206400 and created_at < 1635984000
				and server_id = 'S17' 
				and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
		where rn = 1) t1
group by server_id, base_level, login_date) t2
group by server_id, base_level,'登出'



