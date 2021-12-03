需求：活跃用户野怪击败情况
需求目的：研发新功能数据支持
筛选条件：最近3天连续登录的用户，历史最高击杀的野怪等级分布
数据格式：玩家等级，最高击败野怪ID（按推荐战力排序），人数
 @小乔 


select t1.role_id,t2.battle_id,base_level,type
from
(
    select role_id,count(distinct log_date) as log_days
    from
    (select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
        from log_login_role
        where created_at>=1635724800 and created_at<1635984000
        and role_id in (select uuid from roles where is_internal=false and server_id!='1')
    )as tmp
    group by role_id
)as t1
inner join
(
    select role_id,battle_id,base_level,type --每个玩家在不同等级下击杀的每个野怪类型的最大id
    from
    (
    select role_id,battle_id,base_level,type, 
    row_number() over(partition by role_id,type,base_level order by battle_id desc) as rank2
    from
    (
    select tmp1.role_id,battle_id,tmp2.base_level,type,act_date,
     row_number() over(partition by tmp1.role_id,action_id order by tmp2.created_at desc) as rank --取每次行军前前一次登录的等级
    from
    (
     select role_id,battle_id,base_level,action_id,created_at,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') act_date,
     case when (try_cast(battle_id as int) between 9400 and 9414) then 'type1'
         when (try_cast(battle_id as int) between 9600 and 9614) then 'type2'
         when (try_cast(battle_id as int) between 9700 and 9714) then 'type3'
         when (try_cast(battle_id as int) between 9800 and 9814) then 'type4'
         when (try_cast(battle_id as int) between 9900 and 9914) then 'type5'
         end as type
     from log_missions
     where category=4
     and extend_2='1' and is_return=0 --战斗成功
     and (try_cast(battle_id as int) between 9400 and 9414)
     or (try_cast(battle_id as int) between 9600 and 9614)
     or (try_cast(battle_id as int) between 9700 and 9714)
     or (try_cast(battle_id as int) between 9800 and 9814)
     or (try_cast(battle_id as int) between 9900 and 9914)
  ) as tmp1
    inner join (
        select role_id,base_level,created_at,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') as log_date
        from log_login_role
        )as tmp2 on tmp1.role_id=tmp2.role_id and log_date=act_date
    where tmp1.created_at >= tmp2.created_at
) as d1 where rank=1
    ) as d2 where rank2=1
 )as t2
on t1.role_id=t2.role_id
where log_days=3 


