需求：联盟活跃人数
需求目的：优化跨服战匹配规则
筛选条件：开服六周以上，当前人数大于等于30的所有联盟 ，10.18-10.20
数据结构：日期，服务器，联盟ID，联盟战力，联盟人数，活跃人数，参与跨服战人数（10.15）
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


select daily,t1.alliance_id,t1.server_id,alliance_roles,alliance_power,uv as act_uv,join_uv,mis_uv
from
(
select alliance_id,server_id,alliance_roles,alliance_power,daily
from
(
select *, row_number() over(partition by alliance_id,daily order by created_at) as rank1
from
( select log_update_alliances.alliance_id,log_update_alliances.server_id,alliance_roles,alliance_power,created_at,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as daily
           
    from log_update_alliances inner join alliance on alliance.alliance_id=log_update_alliances.alliance_id
    where created_at>=1634515200 and created_at<1634774400

)as tmp1
)as tmp2 where rank1=1

)as t1

left join (
    select count(distinct role_id) as uv,roles.alliance_id,
        date_format(from_unixtime(log_login_role.created_at) at time zone 'UTC' ,'%Y-%m-%d') as act_day
    from roles 
   INNER JOIN log_login_role ON log_login_role.role_id=roles.uuid
    where log_login_role.created_at>=1634515200 and log_login_role.created_at<1634774400
    and month >= 9 
    and roles.server_id!='1' and is_internal=false
    group by alliance_id, date_format(from_unixtime(log_login_role.created_at) at time zone 'UTC' ,'%Y-%m-%d')
) as t2 on t1.alliance_id=t2.alliance_id  and daily = act_day

left join
(   select count(distinct role_id) as join_uv, alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') join_day
    from log_game_step
     where created_at >= 1634256000 and created_at < 1634342400
         and step_type = 'battle_field'
         and step_id = '1'
         and role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
         group by alliance_id, date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t3 on t1.alliance_id=t3.alliance_id  

left join 
(select count(distinct role_id) as mis_uv, alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') mis_day
                    from log_missions
                    where created_at >= 1634256000 and created_at < 1634342400
                    and extend_3 = 'battle_field' 
                    and role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
                    group by alliance_id, date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
)as t4 on t1.alliance_id=t4.alliance_id  





