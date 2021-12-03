提数需求
1.需求：玩家英雄的培养情况
2.需求目的：分析新一批的英雄上线以后 玩家多队列英雄的培养情况
3.筛选条件
最近7日活跃过的玩家，且等级13级以上的玩家，唯一设备id，橙色特化蚁排除(1000122, 1000123, 1000124, 1000125, 1000126, 1000127)这六只
4.数据结构(csv格式文件就好)
玩家id 服务器 等级 历史充值总额 注册日期 初级协同作战解锁数 中级协同作战解锁数 高级协同作战解锁数 橙色特化蚁 特化蚁等级 技能1等级 技能2等级 技能3等级 技能4等级 技能5等级 技能6等级 技能7等级 技能8等级
 @小乔 
 
 截止北京时间14号下午5点




with major_users as
(
    select uuid,server_id,reg_day,base_level,last_login_day,paid
    from
(
select uuid,paid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day 
        ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles 
where base_level>=13 and last_login_at>=1633564800
and is_internal=false and server_id!='1'
 ) as a
inner join 
(
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,
            row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
    ) b on a.uuid=b.role_id
)


select uuid,server_id,reg_day,base_level,last_login_day,paid,primary,junior,senior,b.hero_id,b.hero_level,hero_type,skill_id,skill_level 
from
(
select  uuid,server_id,reg_day,base_level,last_login_day,paid,
        sum(case when type='初级协同作战' and nums is not null then nums else 0 end) as primary,
        sum(case when type='中级协同作战' and nums is not null then nums else 0 end) as junior,
        sum(case when type='高级协同作战' and nums is not null then nums else 0 end) as senior

from major_users
left join
(
select role_id,
        '初级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7030100,7040100,7070100,7080100)
        and action in ('6401' ,'6202','1210')

group by role_id,
        '初级协同作战'


union

select role_id,
        '中级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7030800,7040800,7070800,7080800)
        and action in ('6401' ,'6202','1210')

group by role_id,
        '中级协同作战'

union

select role_id,
        '高级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7031500,7041500,7071500,7081500)
        and action in ('6401' ,'6202','1210')

group by role_id,
        '高级协同作战'


  
) as t on t.role_id=major_users.uuid     
group by  uuid,server_id,reg_day,base_level,last_login_day,paid

) as a
left join (
select t2.role_id,t2.hero_id,t2.hero_level,t2.hero_type,skill_id,skill_level 
from
(
    select role_id,hero_id,hero_level,hero_type,skill_id,skill_level 
    from(
    select role_id,hero_id,hero_type,skill_id,skill_level ,hero_level
            ,row_number() over(partition by role_id,hero_id,skill_id order by skill_level desc) as rank
    from log_hero_skill 
    where action in ('2900','2901','2902')
    and hero_id not in (1000122, 1000123, 1000124, 1000125, 1000126, 1000127)
    and try_cast(hero_id as int) between 1000101 and 1000179 
    and role_id in (select uuid from major_users)
    and hero_type=4
    ) as tmp
    where rank=1
) as t1 right join (
  select role_id,hero_id,hero_level,hero_type 
    from(
    select role_id,hero_id,hero_type,hero_level
            ,row_number() over(partition by role_id,hero_id order by created_at desc) as rank
    from log_hero
    where 
     hero_id not in (1000122, 1000123, 1000124, 1000125, 1000126, 1000127)
    and try_cast(hero_id as int) between 1000101 and 1000179
    and role_id in (select uuid from major_users)
    and hero_type=4
) as tmp where rank=1 
)as t2 on t1.role_id=t2.role_id and t1.hero_id=t2.hero_id

) as b on a.uuid=b.role_id




