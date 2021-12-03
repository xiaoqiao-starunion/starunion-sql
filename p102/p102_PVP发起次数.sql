需求：PVP发起次数
需求目的：分析开服45天后的PVP发生频率
筛选条件：7月开服的服务器，截止到10.19的数据
数据结构：日期，服务器，DAU，唯一设备DAU，发起侦查的DAU，发起PVP攻击的DAU,被攻击的唯一设备DAU
 @小乔 


with server as
(
 select uuid,date_format(from_unixtime(created_at) at time zone 'UTC' ,'%Y-%m-%d') as open_day
 from servers
 where  created_at>=1625097600 and created_at<1627776000--7月开服
 )



SELECT  t2.daily,t2.server_id,open_day,diff,DAU,"唯一设备dau",
     COALESCE( "发起侦查的DAU", 0 )   "发起侦查的DAU",
     COALESCE( "发起pvp攻击dau", 0 )  "发起pvp攻击dau",
     COALESCE( "集结攻击dau", 0 )     "集结攻击dau",
     COALESCE( "被个人攻击的唯一设备DAU", 0 )  "被个人攻击的唯一设备DAU",
     COALESCE( "被集结攻击的唯一设备DAU", 0 )  "被集结攻击的唯一设备DAU"
FROM
(
SELECT daily,t1.server_id,DAU,"唯一设备dau",open_day,
        date_diff('day',date_parse(open_day,'%Y-%m-%d'),date_parse(daily,'%Y-%m-%d')) as diff
FROM
(
SELECT active_user.daily AS daily ,active_user.server_id,
    COALESCE( active_user.val, 0 ) DAU,
    COALESCE( active_device.val, 0 ) "唯一设备dau"
FROM
    (
    SELECT
        server_id,
        DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
        count( DISTINCT log_login_role.role_id ) val 
    FROM
        log_login_role
    where role_id in (select uuid from roles where is_internal=false and server_id!='1')
        and created_at<1634688000
        and month>=7
    GROUP BY server_id,
    DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
    active_user
    left join
    (
    SELECT
        server_id,
        DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
        count( DISTINCT log_login_role.device_id ) val 
    FROM
        log_login_role
    where role_id in (select uuid from roles where is_internal=false and server_id!='1')
        and created_at<1634688000
        and month>=7
    GROUP BY
        server_id,
    DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
    active_device 
    ON active_device.daily = active_user.daily and active_device.server_id = active_user.server_id
    
) as t1
inner join server on server.uuid=t1.server_id
-- where date_diff('day',date_parse(open_day,'%Y-%m-%d'),date_parse(daily,'%Y-%m-%d'))>=45
) as t2
left join (
    select count(distinct role_id) as "发起侦查的DAU",server_id,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily
    from log_missions
    where category=1 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
     and is_return=0
  and month>=7
    group by server_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
)as t3 on t2.daily=t3.daily and t2.server_id=t3.server_id
left join
( select count(distinct role_id) as "发起pvp攻击dau",server_id,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily
    from log_missions
    where category=2 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
      and battle_id in (select uuid from roles where is_internal=false and server_id!='1')
      and is_return=0
 and month>=7
    group by server_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
)as t4 on t2.daily=t4.daily and t2.server_id=t4.server_id

left join 
(
    select count(distinct device_id) as "被个人攻击的唯一设备DAU",log_missions.server_id,
        DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily
    from log_missions 
    inner join log_login_role 
    on log_missions.battle_id=log_login_role.role_id
    and DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )=
    DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )

    where category in (2) and battle_id in (select uuid from roles where is_internal=false and server_id!='1')
     and is_return=0 --掠夺，玩家被攻击
  and log_missions.month>=7 and log_login_role.month>=7
    group by log_missions.server_id,DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
)as t5 on t2.daily=t5.daily and t2.server_id=t5.server_id
left join 
(
    select count(distinct device_id) as "被集结攻击的唯一设备DAU",log_missions.server_id,
        DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily
    from log_missions inner join log_login_role 
    on log_missions.battle_id=log_login_role.role_id
    and DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )=
    DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
    where category in (7) and battle_id in (select uuid from roles where is_internal=false and server_id!='1')
     and is_return=0 --集结，玩家被攻击
     and log_missions.extend_1='11' --集结参与者出征
  and log_missions.month>=7 and log_login_role.month>=7
    group by log_missions.server_id,DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
)as t6 on t2.daily=t6.daily and t2.server_id=t6.server_id

left join (
    select count(distinct role_id) as "集结攻击dau",server_id,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily
    from log_missions --集结出征时，攻击对象为玩家
    where category=7 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
     and battle_id in (select uuid from roles where is_internal=false and server_id!='1')
     and is_return=0 and extend_1 in ('0','11') and extend_4 !='is_member' 
     and month>=7
    group by server_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
)as t7 on t2.daily=t7.daily and t2.server_id=t7.server_id



