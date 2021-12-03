需求：新手期被掠夺玩家是否流失
需求目的：分析破罩较早被攻击的玩家留存情况
筛选条件：10.25-10.31注册用户，在线天数大于等于3天
数据格式：玩家ID，注册时间，最后登录时间，登录天数，等级，注册第X天首次被攻击（未被攻击记录为空），
被攻击时的等级，死兵数量，重伤数量，注册第Y天最后一次被攻击，被攻击时的等级，死兵数量，重伤数量
 @小乔 

with users as 
( select uuid,reg_date,last_login_day,login_days,base_level
    from
(
 select uuid,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') reg_date,
            DATE_FORMAT(FROM_UNIXTIME(last_login_at) AT TIME ZONE '+00:00', '%Y-%m-%d') last_login_day,
            base_level
 from roles
 where is_internal=false and server_id!='1'
 and created_at>=1635120000 and created_at<1635724800
 ) as t1
inner join
(
    select role_id,count(distinct DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d'))as  login_days
    from log_login_role
    where  year=2021 and month>=10 
    group by role_id
) as t2 on t1.uuid=t2.role_id
where login_days>=3
)



select uuid,reg_date,last_login_day,login_days,users.base_level,a.battle_date as first_battle_date,a.base_level as first_base_level,
        c.injured_nums,c.dead_nums,
        b.battle_date as last_battle_date,b.base_level as last_base_level,d.injured_nums,d.dead_nums
from users
left join
( select battle_id,base_level,battle_date
    from
    (
    select battle_id,base_level,battle_date,row_number() over(partition by role_id order by t2.created_at desc ) as rank2
    from(
    select battle_id,battle_date,created_at
    from(
        select battle_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') battle_date,created_at,
                row_number() over(partition by role_id order by created_at ) as rank
        from log_missions
        where category=2 and is_return=0 and extend_2 in ('1','2')
        and year=2021 and month>=10 
        ) as tmp where rank=1 --首次被攻击
    ) as t1 
    left join log_logout t2
   on t1.battle_id=t2.role_id  where t2.created_at<=t1.created_at --被打之前的最后一次登出时间
   ) as tmp
    where rank2=1
) as a
on a.battle_id=users.uuid
left join
( select battle_id,base_level,battle_date
    from
    (
    select battle_id,base_level,battle_date,row_number() over(partition by role_id order by t2.created_at desc ) as rank2
    from(
    select battle_id,battle_date,created_at
    from(
        select battle_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') battle_date,created_at,
                row_number() over(partition by role_id order by created_at desc) as rank
        from log_missions
        where category=2 and is_return=0 and extend_2 in ('1','2')
        and year=2021 and month>=10 
        ) as tmp where rank=1 --首次被攻击
    ) as t1 
    left join log_logout t2
   on t1.battle_id=t2.role_id  where t2.created_at<=t1.created_at --被打之前的最后一次登出时间
   ) as tmp
    where rank2=1
) as b
on b.battle_id=users.uuid
left join (
 select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as troops_day ,
                sum(if(change_type='2',nums,0)) as injured_nums,
                sum(if(change_type='3',nums,0)) as dead_nums

    from log_troops
    
    where action='6105' and nums>0
    and change_type in ('2','3') and year=2021 and month>=10 
    group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as c on c.role_id=users.uuid and c.troops_day=a.battle_date 
left join (
 select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as troops_day ,
                sum(if(change_type='2',nums,0)) as injured_nums,
                sum(if(change_type='3',nums,0)) as dead_nums

    from log_troops
    where
     action='6105' and nums>0
    and change_type in ('2','3') and year=2021 and month>=10 
    group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as d on d.role_id=users.uuid and d.troops_day=b.battle_date 

