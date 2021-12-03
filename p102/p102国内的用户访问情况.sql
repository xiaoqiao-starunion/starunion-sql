/*
1.需求：国内的用户访问情况
2.需求目的：筛选国内的打广告用户
3.筛选条件
登录ip在大陆有过登录记录，或有登录ip有多个国家的帐号
4.数据结构
帐号id 用户id 设备id 服务器 等级 登录国家或地区计数 是否在大陆有过登录 注册时间 最后登录时间 历史充值金额金额 在线时长 在线天数 
最高重复发言次数 最高单次登录时长
*/


select replace(ac.uuid, 'PAccount:1002:', '') account_id, u.uuid, ac.device_id, u.server_id, u.base_level, login_ip_cnt, 
if(login_cn.role_id is not null, 1, 0) is_login_cn, u.reg_date, u.last_login_date, u.paid, ses.zxsc, login.login_days,
msg.max_msg_cnt, ses.max_zxsc
from (select uuid, account_id, language, base_level, server_id, paid, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') reg_date,
			 date_format(from_unixtime(last_login_at) at time zone 'UTC' ,'%Y-%m-%d') last_login_date
		from roles
		where server_id != '1' and is_internal = false) u
inner join (select t.*
			from (select uuid, country, server_id, device_id  -- 登录IP有多个国家的账号
					from accounts
					where ip in (select ip from accounts group by ip having count(distinct country) > 1)
					union all
					-- 有过大陆登录记录
					select a.uuid, a.country, a.server_id, a.device_id
					from 
					(
						select uuid, country, server_id, device_id
					from accounts
					) a
					inner join 
					(
						select distinct acc_id
					from log_login_role
					where ip_source = 'CN'
					) l on a.uuid = l.acc_id
			) t) ac on u.account_id = ac.uuid
-- 登录国家或地区数量
left join (select role_id, count(distinct ip_source) login_ip_cnt
			from log_login_role
			group by role_id) login_ip on u.uuid = login_ip.role_id
-- 是否在大陆登录
left join (select distinct role_id
			from log_login_role
			where ip_source = 'CN') login_cn on u.uuid = login_cn.role_id 
-- 在线时长
left join (select role_id, sum(secs) zxsc, max(secs) max_zxsc
			from log_session
			group by role_id) ses on u.uuid = ses.role_id
-- 在线天数
left join (select role_id, count(login_date) login_days
			from (select role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') login_date
					from log_login_role
					group by role_id, date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d'))
			group by role_id) login on u.uuid = login.role_id
-- 最高重复发言次数
left join (select role_id, max(msg_cnt) max_msg_cnt
			from (select role_id, msg, count(*) msg_cnt
					from log_role_msg
					group by role_id, msg
					having count(*) > 1)
			group by role_id) msg on u.uuid = msg.role_id



