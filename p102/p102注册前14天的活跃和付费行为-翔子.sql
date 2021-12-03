/*
需求：注册前14天的活跃和付费行为
需求目的：分析开服前两周玩家活跃和充值行为
筛选条件：10.25-10.31注册，且14日留存用户在前14天的活跃和付费情况
数据格式1（活跃行为）：玩家ID，区服，注册时间，国家，最后一次登录时间，是否是滚服用户，注册第X天（每个用户1-14天的数据），在线时长，
发言次数，联盟捐献次数，登录时等级，城建次数，移动建筑次数，科技次数，打野怪次数，掠夺次数，集结次数，采集次数，特化蚁最高等级，
孵化次数，拥有橙色特化蚁数量，孢子消耗
数据格式2（付费行为）：玩家ID，区服，注册时间，国家，最后一次登录时间，是否是滚服用户，注册第X天（每个用户1-14天的数据），充值金额，
充值礼包ID，充值时等级
*/

with users as 
(
select uuid, server_id, country, reg_date, last_login_date, if(dev.last_login_device_id is not null, 1, 0) roll_server,
date_diff('day', date_parse(act_user.reg_date, '%Y-%m-%d'), date_parse(ses.ses_date, '%Y-%m-%d')) diff_day, zxsc, ses_date
from (select distinct uuid, server_id, country, last_login_device_id, reg_date, last_login_date
		from (select uuid, server_id, country, last_login_device_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') reg_date,
				date_format(from_unixtime(last_login_at) at time zone 'UTC' ,'%Y-%m-%d') last_login_date
				from roles 
				where server_id != '1' and is_internal = false
				and created_at >= 1635120000 and created_at < 1635724800) u
		inner join (select distinct role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') login_date
					from log_login_role
					where created_at >= 1635120000
					and year = 2021 and month >= 10) l on u.uuid = l.role_id
		where l.login_date > u.reg_date
		and date_diff('day', date_parse(u.reg_date, '%Y-%m-%d'), date_parse(l.login_date, '%Y-%m-%d')) >= 14) act_user
left join (select last_login_device_id
			from (select uuid, last_login_device_id, server_id, created_at, temp_roles.*
					from roles
					inner join (select tag_device_id, tag_time, tag_server_id
								from (SELECT last_login_device_id AS tag_device_id, created_at AS tag_time,	server_id AS tag_server_id, row_number() over ( PARTITION BY last_login_device_id ORDER BY created_at ) rn 
										FROM roles 
										WHERE LEVEL != 0 )
								where rn = 1) temp_roles on roles.last_login_device_id = temp_roles.tag_device_id
			      where uuid = '19775662')
			where created_at != tag_time 
			and server_id != tag_server_id) dev on act_user.last_login_device_id = dev.last_login_device_id
-- 在线时长
inner join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') ses_date, sum(secs) zxsc
			from log_session
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and extend_1 != 'battle_field'
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) ses on act_user.uuid = ses.role_id
where date_diff('day', date_parse(act_user.reg_date, '%Y-%m-%d'), date_parse(ses.ses_date, '%Y-%m-%d')) <= 13
)



-- 在线时长
select u.*, msg_times, jxcs, base_level, "建城次数", "移动建筑次数",
tech_times, dy_times, ld_times, jj_times, cj_times, hero_level, fh_times, type_4_cnt, sum_nums
from users u
-- 发言
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') msg_date, count(role_id) msg_times
			from log_role_msg
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) msg on u.uuid = msg.role_id and u.ses_date = msg.msg_date
-- 联盟捐献次数
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') res_date, count(role_id) jxcs
			from log_resource
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and action in ('1010', '1011')
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) res on u.uuid = res.role_id and u.ses_date = res.res_date
-- 登录时等级
left join (select role_id, base_level, login_date
			from (select role_id, base_level, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') login_date,
						 row_number() over(partition by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') order by created_at ) rn
					from log_login_role
					where created_at >= 1635120000 and created_at < 1636934400
					and year = 2021 and month >= 10)
			where rn = 1) login on u.uuid = login.role_id and u.ses_date = login.login_date
-- 城建次数
left join (select role_id, sum(case when action in ('6202', '6201') then build_times end) as "建城次数",
				   sum(case when action = '6203' then build_times end) as "移动建筑次数", build_date
			from (select role_id, action, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') build_date, count(role_id) build_times
					from log_buildings
					where created_at >= 1635120000 and created_at < 1636934400
					and year = 2021 and month >= 10
					and action in ('6202', '6201', '6203')
					group by role_id, action, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d'))
			group by role_id, build_date) build on u.uuid = build.role_id and u.ses_date = build.build_date
-- 科技次数
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') tech_date, count(role_id) tech_times
			from log_tech
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and action in ('1210', '6401')
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) tech on u.uuid = tech.role_id and u.ses_date = tech.tech_date
-- 打野怪
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') dy_date, count(role_id) dy_times
			from log_missions
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and category = 4
			and is_return = 0
			and battle_id not in (select uuid from roles)
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) dy on u.uuid = dy.role_id and u.ses_date = dy.dy_date
-- 掠夺次数
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') ld_date, count(role_id) ld_times
			from log_missions
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and category = 2
			and is_return = 0
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) ld on u.uuid = ld.role_id and u.ses_date = ld.ld_date
-- 集结次数
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') jj_date, count(role_id) jj_times
			from log_missions
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and category = 7
			and extend_1 = '2' -- 发起集结
			and is_return = 0
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) jj on u.uuid = jj.role_id and u.ses_date = jj.jj_date
-- 采集次数
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') cj_date, count(role_id) cj_times
			from log_missions
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and category = 3
			and is_return = 0
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) cj on u.uuid = cj.role_id and u.ses_date = cj.cj_date
-- 特化蚁最高等级
left join (select role_id, hero_level, hero_date
			from (select role_id, hero_id, hero_level, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') hero_date,
						 row_number() over(partition by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') order by hero_level desc) rn 
					from log_hero
					where created_at >= 1635120000 and created_at < 1636934400
					and year = 2021 and month >= 10)
			where rn = 1) hero on u.uuid = hero.role_id and u.ses_date = hero.hero_date
-- 孵化次数
left join (select role_id, fh_date, count(role_id) fh_times
			from (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') fh_date
					from log_item
					where created_at >= 1635120000 and created_at < 1636934400
					and year = 2021 and month >= 10
					and action in ('5501','5503','5505')
					and nums < 0
					union all
					select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') fh_date
					from log_item
					where created_at >= 1635120000 and created_at < 1636934400
					and year = 2021 and month >= 10
					and action in ('5502','5504','5506')
					and nums > 0
					union all
					select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') fh_date
					from log_hero
					where created_at >= 1635120000 and created_at < 1636934400
					and year = 2021 and month >= 10
					and action in ('5502','5504','5506'))
			group by role_id, fh_date) fh on u.uuid = fh.role_id and u.ses_date = fh.fh_date
-- 拥有橙色蚁数量
left join (select role_id, hero_date, count(distinct hero_id) type_4_cnt
			from (select h.role_id, ses_date as hero_date, hero_id
					from log_hero h
					inner join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') ses_date
								from log_session 
								where created_at >= 1635120000 and created_at < 1636934400
								and year = 2021 and month >= 10
								and extend_1 != 'battle_field'
								group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) ses on h.role_id = ses.role_id
					where created_at >= 1635120000 and created_at < 1636934400
					and year = 2021 and month >= 10
					and hero_type = 4
					and date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') <= ses.ses_date)
			group by role_id, hero_date) type_4 on u.uuid = type_4.role_id and u.ses_date = type_4.hero_date
-- 孢子消耗
left join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') bz_date, sum(nums) sum_nums
			from log_item
			where created_at >= 1635120000 and created_at < 1636934400
			and year = 2021 and month >= 10
			and item_id = 204067
			and nums < 0
			group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) bz on u.uuid = bz.role_id and u.ses_date = bz.bz_date






/*
数据格式2（付费行为）：玩家ID，区服，注册时间，国家，最后一次登录时间，是否是滚服用户，注册第X天（每个用户1-14天的数据），充值金额，
充值礼包ID，充值时等级
*/
select u.*, sum_price, conf_id, pay_base_level
from users u
left join (select role_id, ses_date, sum(price) sum_price
			from (select p.role_id, base_level, price, conf_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') pay_date, ses_date
					from payments p
					inner join (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') ses_date
								from log_session 
								where created_at >= 1635120000 and created_at < 1636934400
								and year = 2021 and month >= 10
								and extend_1 != 'battle_field'
								group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d')) ses on p.role_id = ses.role_id
					where created_at >= 1635120000 and created_at < 1636934400
					and is_test = 2 and is_paid = 1 and status = 2
					and date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') <= ses.ses_date)
			group by role_id, ses_date) price on u.uuid = price.role_id and u.ses_date = price.ses_date
left join (select role_id, price, base_level as pay_base_level, conf_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') pay_date
			from payments
			where created_at >= 1635120000 and created_at < 1636934400
			and is_test = 2 and is_paid = 1 and status = 2) pay on u.uuid = pay.role_id and u.ses_date = pay.pay_date



