/*
1.需求：
游戏内玩家当前已拥有的五星英雄的强化等级数据统计
强化等级字段：hero_intensify_level
2.需求目的：
分析当前玩家对于各个五星英雄的英雄碎片需求度
3.筛选条件：
目标玩家：UTC时间 10.10 0:00 - 10.11 23:59 的所有的活跃玩家，去内玩
注册时间：UTC时间 10.10 23:59前
服务器：S1-S12
4.数据格式：
输出格式参考如下图，五星英雄各强化等级的玩家人数分布
*/
select hero_id, 
sum(case when hero_intensify_level = 1 then user_cnt end )as plus_1,
sum(case when hero_intensify_level = 2 then user_cnt end )as plus_2,
sum(case when hero_intensify_level = 3 then user_cnt end )as plus_3,
sum(case when hero_intensify_level = 4 then user_cnt end )as plus_4,
sum(case when hero_intensify_level = 5 then user_cnt end )as plus_5,
sum(case when hero_intensify_level = 6 then user_cnt end )as plus_6,
sum(case when hero_intensify_level = 7 then user_cnt end )as plus_7,
sum(case when hero_intensify_level = 8 then user_cnt end )as plus_8
from (select hero_id, hero_intensify_level, count(distinct role_id) user_cnt
from (select users.role_id, hero_id, hero_star, hero_intensify_level
	from (
		select role_id, hero_id, hero_star, hero_intensify_level
		from (
			select role_id, hero_id, hero_star, hero_intensify_level, 
			row_number() over(partition by role_id, hero_id order by created_at desc) rn
					from log_hero 
					where hero_star = 5
					)
					where rn = 1) hero
				inner join (
					select distinct role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS login_date
					from log_login_role login
					inner join (
						select uuid, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS reg_date
						from roles 
						where cast(split(server_id ,'S')[2] AS integer)<=12 
						and is_internal=false and created_at<1633910400
						) u on login.role_id = u.uuid
						where created_at>=1633824000 and created_at<1633996800 and year=2021 and month=10) users 
				on hero.role_id = users.role_id
) temp 
group by hero_id, hero_intensify_level
) t group by hero_id;