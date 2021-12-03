需求：美国用户行军行为-迁服
需求目的：分析迁服前后行军行为变化
筛选条件：时间：3月至今，服务器：2-103服。
9.6-9.10迁服后的玩家归属到原服务器
数据格式：
日期，服务器，DAU，唯一设备DAU，行军类型，参与人数，参与次数
 @小乔 


--这种计算方式会把在9.6-9.10之间没有参与迁服活动但是新手阶段迁进来的玩家算进去，比如9.6=9.10，从5-10，没有参加活动，会把他的原始服判定为10.

with users as
(
select uuid as role_id,if(last_server_id is not null,last_server_id,server_id) as server_id,country,created_at,account_id
from
(
select uuid,server_id,country,created_at,account_id -- 每个用户的最新ID
from roles
where is_internal = false and server_id!='1' and country='US'
) as t1
left join
(
select  role_id,last_server_id,server_id as new_server,
DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%I:%S' ) transfer_date
    from log_fly
    where fly_category=204147 and year=2021 and month=9
    and created_at >=1630886400 and  created_at <1631318400

) as t2 on t1.uuid=t2.role_id
)

select a.server_id,a.login_day,dau,device_dau,category,uv,pv
from
(
select server_id,login_day,count(distinct t2.role_id) as dau,
                      count(distinct t2.device_id) as device_dau
from
(
select role_id,server_id
from users
where try_cast(server_id as int) between 2 and 103
) as t1 
inner join (
select distinct role_id,device_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) login_day
from log_login_role

)as t2 on t1.role_id=t2.role_id
group by server_id,login_day
) as a left join
(

select server_id,act_day,category,count(distinct t3.role_id) as uv,
                            sum(pv) as pv
from
(
select role_id,server_id
from users
where try_cast(server_id as int) between 2 and 103
) as t1 
inner join (
select role_id,category,act_day,count(role_id) as pv
from
  (select role_id,category,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) act_day
  from log_missions
  where is_return=0
  ) as tmp group by role_id,category,act_day
)as t3 on t1.role_id=t3.role_id
group by server_id,act_day,category

) as b on a.server_id=b.server_id and a.login_day=b.act_day