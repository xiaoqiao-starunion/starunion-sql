提数需求
1.需求：美国服联盟数据，美国服玩家活跃数据
2.需求目的：分析美国服联盟结构
3.筛选条件一（美国服联盟数据）：
时间：开服时间3个月以上，最近一周数据，服务器：美国服
数据结构一：
日期，服务器，联盟ID ，充值金额，联盟战力，玩家数，活跃玩家数，联盟玩家总在线时长

筛选条件二（美国服玩家活跃数据）：
时间：最近一周活跃的玩家，服务器：美国服，唯一设备ID
数据结构二：
日期，服务器，玩家ID，创建时间，国家，等级，历史充值金额，玩家战力，所属联盟，在线时长
 @小乔 

 --需求一
 with server as 
 (
    select server_id,country,uv as us_uv --美国人最多的服务器
    from
    (
    select *
    from
    (select server_id,country,uv,row_number() over(partition by server_id order by uv desc ) as rank
    from
    (
    select server_id,country,count(distinct user.uuid) as uv
    from
    (
    select uuid ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) open_day
    from servers
    where created_at <=1625702400
    ) as server inner join
    ( select uuid,country,server_id
    from roles
    )as user on server.uuid=user.server_id
    group by server_id,country
    ) as t1
    ) as t2 where rank=1 

    )as t3 where country='US'
)


select t1.server_id,t1.alliance_id,alliance_power,alliance_roles,alliance_total_paid,login_day,log_uv,online_time_total
from
(   select ali.server_id,alliance_id,alliance_power,alliance_roles,alliance_total_paid
    from
    (
    SELECT alliance_id,alliance_power,alliances.server_id,
            SUM(paid) as  alliance_total_paid,
            count(distinct uuid) as alliance_roles
    FROM alliances
    LEFT JOIN
      (SELECT uuid,roles.alliance_id AS roles_alli_id,
              roles.paid
       FROM roles
       ) AS TEMP 
       ON TEMP.roles_alli_id = alliances.alliance_id
       group by alliance_id,alliance_power,alliance_roles,alliances.server_id
    ) as ali
    inner join server
    on server.server_id=ali.server_id
    

) as t1
left join
(
    select login_day,server_id,alliance_id,count(distinct uuid) as log_uv,sum(online_time) as online_time_total
    from
        ( select uuid,server_id,alliance_id
            from roles
            where is_internal=false and server_id!='1'
        )as t2
        inner join
        (select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) login_day,
            sum(secs)/60 as online_time
            from log_session
            where created_at >= 1632441600  --9.24
            group by  role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
        )as t3 on t2.uuid=t3.role_id
        group by login_day,server_id,alliance_id
) as t4
on t1.server_id=t4.server_id and t1.alliance_id=t4.alliance_id



--需求二、
筛选条件二（美国服玩家活跃数据）：
时间：最近一周活跃的玩家，服务器：美国服，唯一设备ID
数据结构二：
日期，服务器，玩家ID，创建时间，国家，等级，历史充值金额，玩家战力，所属联盟，在线时长
 @小乔 

  with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day,paid,country,power,alliance_id
        from
(
select uuid,server_id,paid,country,power,alliance_id
        ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where  is_internal=false and server_id!='1'

 ) as a

inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        
        ) temp1 where rank=1
        
    ) b on a.uuid=b.role_id

inner join 
( select distinct role_id
    from log_login_role
    where created_at<1633046400 and created_at >= 1632441600  --9.24-9.30 活跃
 and year=2021 and month>=9 
) as c on a.uuid=c.role_id
),


  server as 
 (
    select server_id,country,uv as us_uv --美国人最多的服务器
    from
    (
    select *
    from
    (select server_id,country,uv,row_number() over(partition by server_id order by uv desc ) as rank
    from
    (
    select server_id,country,count(distinct user.uuid) as uv
    from
    (
    select uuid ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) open_day
    from servers
    
    ) as server inner join
    ( select uuid,country,server_id
    from roles
    )as user on server.uuid=user.server_id
    group by server_id,country
    ) as t1
    ) as t2 where rank=1 

    )as t3 where country='US'
)


select uuid,t1.server_id,reg_day,base_level,last_login_day,paid,country,power,alliance_id,login_day,online_time
from
(   
      SELECT  uuid,major_users.server_id,reg_day,base_level,last_login_day,paid,major_users.country,power,alliance_id
       FROM major_users inner join server
       on major_users.server_id=server.server_id
) as t1
left join
(
    select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) login_day,
            sum(secs)/60 as online_time
    from log_session
    where created_at<1633046400 and created_at >= 1632441600  --9.24-9.30 
    group by  role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
)as t2 on t1.uuid=t2.role_id