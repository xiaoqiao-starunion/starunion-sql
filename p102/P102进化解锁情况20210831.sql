进化解锁情况：
注册一周以上，等级大于等于8级
表结构：
ID，区服，注册时间，最近登录时间，等级，初级协同解锁情况，中级协同解锁情况，高级协同解锁情况（解锁填1，未解锁填0）
以各个队列聚合计算，行军部队X，初级协同解锁数量，中级协同解锁数量，高级协同解锁数量
进化ID：
兵蚁行军部队I 
        7030100        初级协同作战
        7030800        中级协同作战
        7031500        高级协同作战
特殊兵蚁行军部队        
        7040100        初级协同作战
        7040800        中级协同作战
        7041500        高级协同作战
兵蚁行军部队II        
        7070100        初级协同作战
        7070800        中级协同作战
        7071500        高级协同作战
兵蚁行军部队III        
        7080100        初级协同作战
        7080800        中级协同作战
        7081500        高级协同作战
玩家只要满足任意队伍的初级/中级/高级协同作战解锁后就计入并且去重 @小乔 6401:升级完成 6202:立即完成 ,1210：科技购买升级


with users as
(
select uuid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where created_at<1629763200 --注册时间小于8.24
    
    and is_internal=false and server_id!='1'
    and base_level>=8

 ) 

with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day
        from
(
select uuid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where created_at<1629763200 --注册时间小于8.24
    
    and is_internal=false and server_id!='1'
    and base_level>=8

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

)

select  uuid,server_id,reg_day,base_level,last_login_day,
        sum(case when type='特殊队列' and primary=1 then 1 else 0 end) as primary_t,
        sum(case when type='队列I' and primary=1 then 1 else 0 end) as primary_1,
        sum(case when type='队列II' and primary=1 then 1 else 0 end) as primary_2,
        sum(case when type='队列III' and primary=1 then 1 else 0 end) as primary_3,
        sum(case when type='特殊队列' and junior=1 then 1 else 0 end) as junior_t,
        sum(case when type='队列I' and junior=1 then 1 else 0 end) as junior_1,
        sum(case when type='队列II' and junior=1 then 1 else 0 end) as junior_2,
        sum(case when type='队列III' and junior=1 then 1 else 0 end) as junior_3,
        sum(case when type='特殊队列' and senior=1 then 1 else 0 end) as senior_t,
        sum(case when type='队列I' and senior=1 then 1 else 0 end) as senior_1,
        sum(case when type='队列II' and senior=1 then 1 else 0 end) as senior_2,
        sum(case when type='队列III' and senior=1 then 1 else 0 end) as senior_3

from users
inner join
(
select role_id,
        '队列I' as type,
       sum(if( tech_id=7030100 , 1,0)) as primary,
        sum(if( tech_id=7030800 , 1,0)) as junior,
        sum(if( tech_id=7031500 , 1,0)) as senior

from log_tech 
where tech_id in (7030100,7030800,7031500)
        and action in ('6401' ,'6202')

group by role_id,
        '队列I'


union

select role_id,
        '特殊队列'as type,
       sum(if( tech_id=7040100 , 1,0)) as primary,
        sum(if( tech_id=7040800 , 1,0)) as junior,
        sum(if( tech_id=7041500 , 1,0)) as senior

from log_tech 
where tech_id in (7040100,7040800,7041500)
        and action in ('6401' ,'6202')

group by role_id,
        '特殊队列'

union

select role_id,
        '队列II'as type,
       sum(if( tech_id=7070100 , 1,0)) as primary,
        sum(if( tech_id=7070800 , 1,0)) as junior,
        sum(if( tech_id=7071500 , 1,0)) as senior

from log_tech 
where tech_id in (7070100,7070800,7071500)
        and action in ('6401' ,'6202')

group by role_id,
        '队列II'


union

select role_id,
        '队列III'as type,
       sum(if( tech_id=7080100 , 1,0)) as primary,
        sum(if( tech_id=7080800 , 1,0)) as junior,
        sum(if( tech_id=7081500 , 1,0)) as senior

from log_tech 
where tech_id in (7080100,7080800,7081500)
        and action in ('6401' ,'6202')

group by role_id,
        '队列III'      
) as t on t.role_id=users.uuid          

group by  uuid,server_id,reg_day,base_level,last_login_day




