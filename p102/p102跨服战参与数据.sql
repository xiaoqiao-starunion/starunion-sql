需求：联盟活跃人数
需求目的：优化跨服战匹配规则
筛选条件：开服六周以上，人数大于等于30的所有联盟在10.11-10.22的数据 只能看10.18-10.22
数据结构：
日期，服务器，联盟ID，联盟战力，联盟人数，活跃人数，
参与联盟远征人数（10.8），人均参与时长；
参与联盟远征人数（10.15），人均参与时长；
参与联盟远征人数（10.22），人均参与时长；
 @小乔 



 需求：联盟远征参与数据
需求目的：优化玩法数值
筛选条件：10.22参与联盟远征的所有联盟
数据结构1：服务器，联盟ID，战斗力，联盟人数，参与人数， 占领天然运输通道次数， 占领异香魔芋次数， 采集总得分
数据结构2：参与玩家ID，服务器，等级，总积分，占领积分，杀敌积分，采集积分
 @小乔 


 

with alliance as (

 select server.uuid as server_id,open_day,alliance_id,alliance_roles as uv
from
(
 select uuid,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as open_day
 from servers
 where  created_at<1631059200 --9.8号前开服

 ) as server
inner join (
    select alliance_id,server_id,alliance_roles,row_number() over(partition by alliance_id order by created_at desc) as rank
    from log_update_alliances  --取每个联盟最后一条日志记录，最新人数
) as ali
on server.uuid=ali.server_id
where alliance_roles>=30 and rank=1
)


select t1.daily,t1.alliance_id,t1.server_id,alliance_roles,alliance_power,uv as act_uv,
            t3.mis_uv_1,t3.per_time,mis_uv_2,t4.per_time,mis_uv_3,t5.per_time
from
(
select alliance_id,server_id,alliance_roles,alliance_power,daily
from
(
select *, row_number() over(partition by alliance_id,daily order by created_at) as rank1
from
( select log_update_alliances.alliance_id,log_update_alliances.server_id,alliance_roles,alliance_power,created_at,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as daily
           
    from log_update_alliances inner join alliance on alliance.alliance_id=log_update_alliances.alliance_id
    where created_at>=1634515200 and created_at<1634947200

)as tmp1
)as tmp2 where rank1=1

)as t1

left join (
    select count(distinct role_id) as uv,roles.alliance_id,
        date_format(from_unixtime(log_login_role.created_at) at time zone 'UTC' ,'%Y-%m-%d') as act_day
    from roles 
   INNER JOIN log_login_role ON log_login_role.role_id=roles.uuid
    where log_login_role.created_at>=1634515200 and log_login_role.created_at<1634947200
    and month >= 9 
    and roles.server_id!='1' and is_internal=false
    group by alliance_id, date_format(from_unixtime(log_login_role.created_at) at time zone 'UTC' ,'%Y-%m-%d')
) as t2 on t1.alliance_id=t2.alliance_id  and daily = act_day

left join 
( select count(distinct log_session.role_id) as mis_uv_1,sum(secs) as time_all_1,
        sum(secs) / count(distinct log_session.role_id)/60 as per_time,
        log_missions.alliance_id,
        date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
from log_session inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at >= 1633651200 and log_missions.created_at < 1633737600
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=log_session.role_id 
and date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')=daily

where 
  log_session.created_at >= 1633651200 and log_session.created_at < 1633737600
           
                    and log_session.extend_1 = 'battle_field' 
                    and log_session.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t3 on t1.alliance_id=t3.alliance_id  


left join 
( select count(distinct log_session.role_id) as mis_uv_2,sum(secs) as time_all,
        log_missions.alliance_id,
        sum(secs) / count(distinct log_session.role_id)/60 as per_time,
        date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
from log_session inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  
             log_missions.created_at>=1634256000 and log_missions.created_at<1634342400
            
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=log_session.role_id 
and date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')=daily

where log_session.created_at>=1634256000 and log_session.created_at<1634342400
           
                    and log_session.extend_1 = 'battle_field' 
                    and log_session.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t4 on t1.alliance_id=t4.alliance_id  

left join 
( select count(distinct log_session.role_id) as mis_uv_3,sum(secs) as time_all,
        log_missions.alliance_id,
        sum(secs) / count(distinct log_session.role_id)/60 as per_time,
        date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
from log_session inner join 
( select distinct role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at>=1634860800 and log_missions.created_at<1634947200
                    and log_missions.extend_3 = 'battle_field' 
 )log_missions 
on log_missions.role_id=log_session.role_id 
and date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')=daily

where log_session.created_at>=1634860800 and log_session.created_at<1634947200
                    and log_session.extend_1 = 'battle_field' 
                    and log_session.role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                   
group by log_missions.alliance_id,date_format(FROM_UNIXTIME(log_session.created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t5 on t1.alliance_id=t5.alliance_id  
        



 需求：联盟远征参与数据
需求目的：优化玩法数值
筛选条件：10.22参与联盟远征的所有联盟
数据结构1：服务器，联盟ID，战斗力，联盟人数，参与人数， 占领天然运输通道次数， 占领异香魔芋次数， 采集总得分
数据结构2：参与玩家ID，服务器，等级，总积分，占领积分，杀敌积分，采集积分
 @小乔 

select t1.server_id,t1.alliance_id,t1.daily,alliance_power,alliance_roles,uv,action,nums_type,nums
from
 (
    select server_id,count(distinct role_id) as uv,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at>=1634860800 and log_missions.created_at<1634947200
                    and log_missions.extend_3 = 'battle_field' 
                    and role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                    group by server_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t1
 left join
 ( select alliance_id,server_id,alliance_roles,alliance_power,
    row_number() over(partition by alliance_id order by  created_at) as rank 
    from log_update_alliances
    where  created_at>=1634860800 and created_at<1634947200
)as t2 on t1.alliance_id=t2.alliance_id
 left join
 (
select alliance_id,action,nums_type,nums,server_id
from log_alliance_score
where created_at>=1634860800 and created_at<1634947200
and ac_id='1000118'
)as t3
on t1.alliance_id=t3.alliance_id
where rank=1


select t1.role_id,server_id,base_level,action,nums
from
(
select role_id,server_id,base_level,action,nums
from log_activity_score
where ac_id='1000118' and action in ('9030','9031','9032','9033')
and  created_at>=1634860800 and created_at<1634947200
and role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
) as t1 inner join
( select distinct role_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily
    from log_missions
     where  log_missions.created_at>=1634860800 and log_missions.created_at<1634947200
                    and log_missions.extend_3 = 'battle_field' 
                    and role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                  
)as t2 on t1.role_id=t2.role_id