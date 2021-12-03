 需求：联盟远征参与情况
需求目的：优化跨服战匹配规则
筛选条件：开服六周以上，人数大于等于30的所有联盟在10.29，11.5，11.12的数据
数据结构：
日期，服务器，联盟ID，联盟战力，联盟人数，活跃人数，
参与联盟远征人数（10.29），参与联盟远征的玩家战力，人均参与时长，对阵联盟ID（无法判断）；
参与联盟远征人数（11.5），参与联盟远征的玩家战力，人均参与时长，对阵联盟ID；
参与联盟远征人数（11.12），参与联盟远征的玩家战力，人均参与时长，对阵联盟ID；
 @小乔



with alliance as (

 select server.uuid as server_id,open_day,alliance_id,alliance_roles as uv
from
(
 select uuid,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as open_day
 from servers
 where  created_at<1633046400 --10.1号前开服

 ) as server
inner join (
    select alliance_id,server_id,alliance_roles,row_number() over(partition by alliance_id order by created_at desc) as rank
    from log_update_alliances  --取每个联盟最后一条日志记录，最新人数
) as ali
on server.uuid=ali.server_id
where alliance_roles>=30 and rank=1
)


select t1.daily,t1.alliance_id,t1.server_id,alliance_roles,alliance_power,uv as act_uv,
            t3.mis_uv_1,t3.per_time,t6.power,mis_uv_2,t4.per_time,t7.power,mis_uv_3,t5.per_time,t8.power
from
(
    select alliance_id,server_id,alliance_roles,alliance_power,daily
    from
    (
    select *, row_number() over(partition by alliance_id,daily order by created_at) as rank1
    from
    ( select log_update_alliances.alliance_id,log_update_alliances.server_id,alliance_roles,alliance_power,
        created_at,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as daily
               
        from log_update_alliances inner join alliance on alliance.alliance_id=log_update_alliances.alliance_id
        where created_at>=1635465600 and created_at<1636761600

    )as tmp1
    )as tmp2 where rank1=1 --每个联盟每天的第一条联盟日志

)as t1 

left join (
    select count(distinct role_id) as uv,roles.alliance_id,
        date_format(from_unixtime(log_login_role.created_at) at time zone 'UTC' ,'%Y-%m-%d') as act_day
    from roles 
   INNER JOIN log_login_role ON log_login_role.role_id=roles.uuid
    where log_login_role.created_at>=1635465600 and log_login_role.created_at<1636761600
    and month >= 9 
    and roles.server_id!='1' and is_internal=false
    group by roles.alliance_id, date_format(from_unixtime(log_login_role.created_at) at time zone 'UTC' ,'%Y-%m-%d')
) as t2 on t1.alliance_id=t2.alliance_id  and daily = act_day

left join --10.29
( select count(distinct log_session.role_id) as mis_uv_1,sum(secs) as time_all_1,
        sum(secs) / count(distinct log_session.role_id)/60 as per_time,
        log_missions.alliance_id,
        date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
from log_session inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at >= 1635465600 and log_missions.created_at < 1635552000
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=log_session.role_id 
and date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')=daily

where 
  log_session.created_at >= 1635465600 and log_session.created_at < 1635552000
                    and log_session.extend_1 = 'battle_field' 
                    and log_session.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t3 on t1.alliance_id=t3.alliance_id  

left join 
( select sum(power) as power,alliance_id,daily
    from
    (select uuid,power,row_number() over(partition by uuid order by created_at desc) as rank
from log_update_roles
where created_at < 1635465600 --10.29 00:00
) as tmp1
inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at >= 1635465600 and log_missions.created_at < 1635552000
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=tmp1.uuid 

where rank=1 and
    log_missions.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,daily
)as t6 on t1.alliance_id=t6.alliance_id 




left join --11.5
( select count(distinct log_session.role_id) as mis_uv_2,sum(secs) as time_all,
        log_missions.alliance_id,
        sum(secs) / count(distinct log_session.role_id)/60 as per_time,
        date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
from log_session inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  
             log_missions.created_at>=1636070400 and log_missions.created_at<1636156800
            
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=log_session.role_id 
and date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')=daily

where log_session.created_at>=1636070400 and log_session.created_at<1636156800
           
                    and log_session.extend_1 = 'battle_field' 
                    and log_session.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t4 on t1.alliance_id=t4.alliance_id  

left join 
( select sum(power) as power,alliance_id,daily
    from
    (select uuid,power,row_number() over(partition by uuid order by created_at desc) as rank
from log_update_roles 
where 
 created_at < 1636070400 --11.5 00:00
) as tmp1
inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at >= 1636070400 and log_missions.created_at < 1636156800
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=tmp1.uuid 

where rank=1 and
    log_missions.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,daily
)as t7 on t1.alliance_id=t7.alliance_id 




left join --11.12
( select count(distinct log_session.role_id) as mis_uv_3,sum(secs) as time_all,
        log_missions.alliance_id,
        sum(secs) / count(distinct log_session.role_id)/60 as per_time,
        date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
from log_session inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at>=1636675200 and log_missions.created_at<1636761600
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=log_session.role_id 
and date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')=daily

where log_session.created_at>=1636675200 and log_session.created_at<1636761600
                    and log_session.extend_1 = 'battle_field' 
                    and log_session.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t5 on t1.alliance_id=t5.alliance_id  
  
left join 
( select sum(power) as power,alliance_id,daily
    from
    (select uuid,power,row_number() over(partition by uuid order by created_at desc) as rank
from log_update_roles 
where 
 created_at < 1636675200 --11.12 00:00
) as tmp1
inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at >= 1636675200 and log_missions.created_at < 1636761600
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=tmp1.uuid 

where rank=1 and
    log_missions.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,daily
)as t8 on t1.alliance_id=t8.alliance_id 
