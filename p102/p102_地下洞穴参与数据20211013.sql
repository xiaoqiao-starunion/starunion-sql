需求：地下洞穴参与数据
需求目的：分析参与情况
筛选条件：日期9.28-10.10
数据结构（地下洞穴参与情况）：
日期，DAU（等级大于等于10级且拥有建筑“4093000”），点击玩法入口的玩家数，参与地下洞穴玩家数（挑战任意关卡），挑战次数，跳转到礼包中心人数，跳转到礼包中心次数，领取每日奖励人数（action=9019）

数据结构（商店兑换情况）：
日期，商店类型（ ac_id=9020 地下试炼首通商店兑换，ac_id=9021 地下试炼水晶商店兑换），参与玩家，兑换道具，兑换数量，兑换人数
数据结构（10.10各等级关卡分布）：
玩家等级，关卡类型，关卡等级，人数
 @小乔 



/*
需求：地下洞穴参与数据
需求目的：分析参与情况
筛选条件：日期9.28-10.10
数据结构（地下洞穴参与情况）：
日期，DAU（等级大于等于10级且拥有建筑“4093000”），点击玩法入口的玩家数，参与地下洞穴玩家数（挑战任意关卡），挑战次数，跳转到礼包中心人数，跳转到礼包中心次数，领取每日奖励人数（action=9019）
*/

with users as 
(
select distinct uuid, reg_date, logout_date
from (select uuid, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') reg_date
                from roles 
                where server_id != '1' and is_internal = false) u 
inner join (select role_id, base_level, logout_date
                         from (select distinct role_id, base_level, created_at, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') logout_date,
                                                    ROW_NUMBER() over(PARTITION by role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') order by created_at desc) rank
                                         from log_logout 
                                         where created_at >= 1632787200 and created_at < 1633996800) t 
                         where rank = 1 and base_level >= 10) logout on u.uuid = logout.role_id
inner join (select distinct role_id, build_conf_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') b_date
                          from log_buildings
                        where action in ('6201', '6202') 
                            and build_conf_id = 4093000) build on u.uuid = build.role_id and build.b_date <= logout.logout_date
),

-- DAU 
dau_info as 
(
select logout_date, count(distinct uuid) dau
from users 
group by logout_date
),
                        
-- 点击玩法入口   
click_info as                   
(
select logout_date, count(distinct click.role_id) click_users
from users u 
left join (select distinct split(split(role_id, ',')[1], ':')[2] as role_id, server_id, step_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') click_date
                      from log_game_step 
                        where created_at >= 1632787200 and created_at < 1633910400 
                        and step_type='undercavern_click'
                        and year = 2021 and month >= 9) click on u.uuid = click.role_id and u.logout_date = click.click_date
group by logout_date
),

-- 参与地下洞穴玩家数（挑战任意关卡），挑战次数
join_info as 
(
select logout_date, count(distinct j.role_id) join_users, sum(join_cnt) sum_join
from users u 
left join (select role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') join_date, count(*) join_cnt
                         from log_puzzle 
                        where created_at >= 1632787200 and created_at < 1633910400 
                        and level_chapter in ('76010000','76020000','76040000','76060000')
                        group by role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d')) j on u.uuid = j.role_id and u.logout_date = j.join_date
group by logout_date
),

-- 跳转到礼包中心次数
jump_info as 
(
select logout_date, count(distinct jump.role_id) jump_users, sum(jump_times) sum_jump
from users u 
left join (select split(split(role_id, ',')[1], ':')[2] as role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') click_date, count(*) jump_times
                      from log_game_step 
                        where created_at >= 1632787200 and created_at < 1633910400 
                        and step_type='undercavern_jump_gift'
                        and year = 2021 and month >= 9
                        group by role_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d')) jump on u.uuid = jump.role_id and u.logout_date = jump.click_date
group by logout_date
),

-- 领取每日奖励人数
award_info as 
(
select logout_date, award.ac_id, count(distinct award.role_id) award_users
from users u 
left join (select distinct role_id, server_id, ac_id, DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') award_date 
                        from log_activity_award
                        where created_at >= 1632787200 and created_at < 1633910400 
                            and year = 2021 and month >= 9
                         and action = '9019') award on u.uuid = award.role_id and u.logout_date = award.award_date
group by u.logout_date, award.ac_id
)


-- 日期，DAU（等级大于等于10级且拥有建筑“4093000”），点击玩法入口的玩家数，参与地下洞穴玩家数（挑战任意关卡），挑战次数，跳转到礼包中心人数，跳转到礼包中心次数，领取每日奖励人数（action=9019）
select a.logout_date, a.dau, b.click_users, c.join_users, c.sum_join, d.jump_users, d.sum_jump, e.ac_id, e.award_users
from dau_info a 
left join click_info b on a.logout_date = b.logout_date 
left join join_info c on a.logout_date = c.logout_date 
left join jump_info d on a.logout_date = d.logout_date 
left join award_info e on a.logout_date = e.logout_date;







--需求二

玩家基本信息、ac_id：活动id，consume_item_id：消耗的道具id（标记水晶）、consume_nums：消耗的道具数量、item_id：兑换的道具id    
log_item_exchange       
ac_id=9020 地下试炼首通商店兑换
ac_id=9021 地下试炼水晶商店兑换
select  t1.act_day,t1.action,uv_all,item_id,uv,pv
from
(
select act_day,action,item_id,count(distinct role_id) as uv,count(role_id) as pv
from
( select role_id,action,item_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) act_day
 from log_item
 where action in ('9020','9021')
 and created_at>=1632787200 and created_at<1633910400
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
)as temp
group by action,item_id,act_day
) as t1
inner join
( 
    select action,act_day,count(distinct role_id) as uv_all
from
( select role_id,action,item_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) act_day
 from log_item
 where action in ('9020','9021')
 and created_at>=1632787200 and created_at<1633910400
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
)as temp
group by action,act_day
) as t2
on t1.act_day=t2.act_day and t1.action=t2.action

--需求三
玩家基本信息、level_chapter：关卡类型、level_id：关卡等级、result：战斗结果、params：上阵英雄

select b.base_level,level_chapter,level_id,count(distinct a.role_id) as uv

from
(   select role_id,base_level,level_chapter,level_id
    from
    (select role_id,base_level,level_chapter,level_id,row_number() over(partition by role_id,level_chapter order by level_id desc) as rank
    from log_puzzle
    where  created_at<1633910400
    and  role_id in (select uuid from roles where is_internal=false and server_id!='1')
    ) as t where rank=1

) as a
inner join 
(select role_id,base_level
    from
     ( select role_id,base_level,row_number() over(partition by role_id order by created_at desc) as rank2
    from log_login_role
where created_at<=1633824000
)as tmp
where rank2=1
)as b on a.role_id=b.role_id
group by b.base_level,level_chapter,level_id

