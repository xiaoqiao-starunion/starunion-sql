/*
提数需求
1.需求：玩家破冰时的数据
2.需求目的：用于训练玩家破冰预测机器学习模型
3.筛选条件
玩家信息第一次付费时的数据
4.数据结构
玩家id 玩家是否改名 国家 工会 t几国家 服务器 注册日期 玩家等级 玩家登录天数 玩家在线时长 玩家习性 军队战力 建筑战力 科技战力 
特化蚂蚁战力 建筑升级点击次数 玩家发言次数 集结参与次数 对野怪发起进攻次数 首充礼包 玩家设备系统 是否绑定第三方
*/


with role_first_paid as 
(
select uuid as role_id, account_id, first_paid_time as first_paid_at, DATE_FORMAT(FROM_UNIXTIME(first_paid_time) AT TIME ZONE '+00:00', '%Y-%m-%d') first_paid_date,
country, server_id, device_os, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') reg_date
from roles 
where server_id != '1' and is_internal = false
and paid > 0
)


select x.role_id, replace(level.acc_id, 'PAccount:1002:', '') account_id, first_paid_at, first_paid_date, if(old_name.role_id is not null, 1, 0) chg_name, pay.conf_id, pay.conf_id_type, pay.price, country, 
level.server_id, level.base_level, login.login_days, s.zxsc, habit.habit, habit.before_habit, al.alliance_id, al.alliance_level, b_power.buildings_power, 
army.armies_power, tech.techs_power, reg_date, hero.hero_power, build.build_upgrade_times, msg.msg_times, mis.mis_times, 
battle.battle_times, level.device_os
from role_first_paid x
left join (select distinct a.role_id
			from log_rename a, role_first_paid b
			where a.role_id = b.role_id
			and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date) old_name on x.role_id = old_name.role_id
-- 登录天数
left join (select role_id, count(login_date) login_days
			from (select a.role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') login_date
					from log_login_role a, role_first_paid b
					where a.role_id = b.role_id
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date
					group by a.role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d')) t
			group by role_id) login on x.role_id = login.role_id
-- 在线时长
left join (select a.role_id, sum(secs) zxsc
			from log_session a, role_first_paid b
			where a.role_id = b.role_id
			and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date
			group by a.role_id) s on x.role_id = s.role_id
-- 习性
left join (select role_id, habit, before_habit, alliance_id
			from (select a.role_id, habit, before_habit, alliance_id, row_number() over(partition by a.role_id order by created_at desc) rn
					from log_habit a, role_first_paid b
					where a.role_id = b.role_id
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date) t
			where rn = 1) habit on x.role_id = habit.role_id
-- 联盟
left join (select role_id, alliance_id, alliance_level
			from (select a.role_id, a.alliance_id, a.alliance_level, row_number() over(partition by a.role_id order by created_at desc) rn
					from log_alliance_change a, role_first_paid b
					where a.role_id = b.role_id
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date)
		  where rn = 1) al on x.role_id = al.role_id
-- 玩家等级
left join (select role_id, base_level, server_id, device_os, acc_id
			from (select a.role_id, a.base_level, a.server_id, a.device_os, a.acc_id,
						 row_number() over(partition by a.role_id order by created_at desc) rn
					from log_logout a, role_first_paid b
					where a.role_id = b.role_id 
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date) t
		   where rn = 1) level on x.role_id = level.role_id
-- 军队战力 6001
left join (select role_id, balance as armies_power
			from (select a.role_id, a.balance, a.alliance_id, row_number() over(partition by a.role_id order by created_at desc) rn
					from log_power_change a, role_first_paid b
					where action = '6001'
					and alliance_id = ''
					and a.role_id = b.role_id
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date)
		  where rn = 1) army on x.role_id = army.role_id
-- 建筑战力 6003
left join (select role_id, balance as buildings_power
			from (select a.role_id, a.balance, a.alliance_id, row_number() over(partition by a.role_id order by created_at desc) rn
					from log_power_change a, role_first_paid b
					where action = '6003'
					and alliance_id = ''
					and a.role_id = b.role_id
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date)
		  where rn = 1) b_power on x.role_id = b_power.role_id
-- 科技战力 6002
left join (select role_id, balance as techs_power
			from (select a.role_id, a.balance, a.alliance_id, row_number() over(partition by a.role_id order by created_at desc) rn
					from log_power_change a, role_first_paid b
					where action = '6002'
					and alliance_id = ''
					and a.role_id = b.role_id
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date)
		  where rn = 1) tech on x.role_id = tech.role_id
-- 特化蚁战力
left join (select role_id, sum(hero_power) hero_power
			from (select a.role_id, hero_id, hero_power, row_number() over(partition by a.role_id, hero_id order by created_at desc) rn
					from log_hero a, role_first_paid b
					where a.role_id = b.role_id
					and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date) t
			where rn = 1
			group by role_id) hero on x.role_id = hero.role_id
-- 升级建筑次数
left join (select a.role_id, count(build_conf_id) build_upgrade_times
			from log_buildings a, role_first_paid b
			where a.role_id = b.role_id
			and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date
			and a.action in ('6201', '6202')
			group by a.role_id) build on x.role_id = build.role_id
-- 发言次数
left join (select a.role_id, count(msg) msg_times
			from log_role_msg a, role_first_paid b
			where a.role_id = b.role_id
			and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date
			group by a.role_id) msg on x.role_id = msg.role_id
-- 集结次数
left join (select a.role_id, count(a.category) mis_times
			from log_missions a, role_first_paid b
			where a.role_id = b.role_id
			and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date
			and a.category = 7
			and a.is_return = 0
			group by a.role_id) mis on x.role_id = mis.role_id
-- 进攻野怪次数
left join (select a.role_id, count(a.battle_id) battle_times
			from log_missions a, role_first_paid b
			where a.role_id = b.role_id
			and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date
			and a.category in (4, 7)
			and a.is_return = 0
			and a.battle_id not in (select uuid from roles where server_id != '1' and is_internal = false)
			group by a.role_id) battle on x.role_id = battle.role_id
-- 首充礼包
left join (select role_id, conf_id, conf_id_type, price
			from (select a.role_id, a.conf_id, a.conf_id_type, a.price,
					 row_number() over(partition by a.role_id order by created_at asc) rn
				from payments a, role_first_paid b
				where a.role_id = b.role_id
				and DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') <= b.first_paid_date
				and a.is_test = 2
				and a.is_paid = 1 
				and a.status = 2) t
		  where rn = 1) pay on x.role_id = pay.role_id



