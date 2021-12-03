需求：联盟人数集中趋势
需求目的：评估官员工资对联盟生态的影响力
筛选条件：分别取7.1-7.15，8.3-8.17月开服的服务器分别在9.30和10.10的联盟数据
数据结构：
服务器，开服时间，联盟ID，排名，人数，联盟前3天活跃人数（取数日期的前三天），战斗力
 @小乔 

 select server.uuid as server_id,open_day,alliance_id,alliance_roles,alliance_power,uv,
        row_number() over(order by alliance_power desc) as rank2
from
(
 select uuid,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as open_day
 from servers
 where created_at>=1625097600 and created_at<1626393600

 ) as server
left join (
    select t1.alliance_id,server_id,alliance_roles,alliance_power,uv
    from
    (select alliance_id,server_id,alliance_roles,alliance_power
    from(
    select alliance_id,server_id,alliance_roles,alliance_power,
            row_number() over(partition by alliance_id order by created_at desc) as rank1
    from log_update_alliances
     where created_at<=1633824000
    )as tmp where rank1=1
)as t1
left join (
    select count(distinct role_id) as uv,roles.alliance_id
    from roles 
   INNER JOIN log_login_role ON log_login_role.role_id=roles.uuid
    where log_login_role.created_at<1633824000 and log_login_role.created_at>=1633564800
    and month >= 9 and roles.server_id!='1' and is_internal=false
    group by alliance_id
) as t2 on t1.alliance_id=t2.alliance_id
) as ali
on server.uuid=ali.server_id



 select server.uuid as server_id,open_day,alliance_id,alliance_roles,alliance_power,uv,
        row_number() over(order by alliance_power desc) as rank2
from
(
 select uuid,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as open_day
 from servers
 where created_at>=1627948800 and created_at<1629244800

 ) as server
left join (
    select t1.alliance_id,server_id,alliance_roles,alliance_power,uv
    from
    (select alliance_id,server_id,alliance_roles,alliance_power
    from(
    select alliance_id,server_id,alliance_roles,alliance_power,
            row_number() over(partition by alliance_id order by created_at desc) as rank1
    from log_update_alliances
    where created_at<=1633824000
    )as tmp where rank1=1
)as t1
left join (
    select count(distinct role_id) as uv,roles.alliance_id
    from roles 
   INNER JOIN log_login_role ON log_login_role.role_id=roles.uuid
    where log_login_role.created_at<1633824000 and log_login_role.created_at>=1633564800
    and month >= 9 and roles.server_id!='1' and is_internal=false
    group by alliance_id
) as t2 on t1.alliance_id=t2.alliance_id
) as ali
on server.uuid=ali.server_id
