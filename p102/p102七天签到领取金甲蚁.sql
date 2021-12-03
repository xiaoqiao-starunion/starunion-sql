/*
1.需求：七天签到领取的奖励人数数据
2.需求目的：分析七天签到领取的奖励人数在更新前后的变化
3.筛选条件
玩家注册时间：8.17-10.3，唯一设备id
4.数据结构
 服务器 玩家id 注册时间 第七天奖励领取领取时间
*/

select u.uuid, u.server_id, u.reg_date, award.get_time
from (select uuid, server_id, date_format(from_unixtime(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') reg_date
				from roles 
			 where server_id != '1' and is_internal = false
				 and created_at >= 1629158400 and created_at < 1633305600
				 ) u
inner join (
	select distinct role_id
	from (
		select role_id, base_level, row_number() over(partition by device_id order by base_level desc) level_rank 
		from log_login_role
		) w 
		where level_rank = 1) max_level 
on u.uuid = max_level.role_id
inner join (
	select role_id, date_format(from_unixtime(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d %H-%i-%s') get_time
	from log_hero
		where action = '4102' and hero_id=1000161
		) award on u.uuid = award.role_id;