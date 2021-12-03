需求：玩家驻防伤兵情况
需求目的：用于分析是否要调整死伤兵机制
筛选条件：11.15-11.21期间登录，且等级大于等于16级，唯一设备ID下最大等级角色
数据格式：日期，DAU（>=16），被攻击玩家数，死伤兵为0的玩家数，死伤兵小于1000的玩家数，死伤兵小于10000的玩家数，死伤兵小于50000的玩家数
 @小乔 再加一个死伤兵5-10万，10-20万，20-30万的玩


with major_users as
(
  select uuid,server_id,reg_day,base_level,paid
  from
(
select uuid,server_id,paid,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where is_internal=false and server_id!='1'
 ) as a
inner join
 (select distinct role_id
    from log_login_role 
    where log_login_role.created_at>=1636934400 and log_login_role.created_at<1637539200
    
    ) as b on a.uuid=b.role_id
inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
        
    ) c on a.uuid=c.role_id and c.role_id=b.role_id

)

select login_day,count(distinct a.role_id) as dau,
    count(distinct b.battle_id) as "被攻击玩家数-活跃",
    count(distinct if(nums=0,battle_id,null)) as "死伤兵为0的玩家数",
    count(distinct if(nums>0 and nums<=1000,battle_id,null)) as "死伤兵0-1000的玩家数",
    count(distinct if(nums>1000 and nums<=10000,battle_id,null)) as "死伤兵1000-10000的玩家数",
    count(distinct if(nums>10000 and nums<=50000,battle_id,null)) as "死伤兵10000-50000的玩家数",
    count(distinct if(nums>50000 and nums<=100000,battle_id,null)) as "死伤兵50000-100000的玩家数",
    count(distinct if(nums>100000 and nums<=200000,battle_id,null)) as "死伤兵100000-200000的玩家数",
    count(distinct if(nums>200000 and nums<=300000,battle_id,null)) as "死伤兵200000-300000的玩家数",
    count(distinct if(nums>300000,battle_id,null)) as "死伤兵300000+的玩家数"
from
(
select login_day,role_id
from major_users
inner join (
    select  distinct role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as login_day 
    from log_login_role 
    where log_login_role.created_at>=1636934400 and log_login_role.created_at<1637539200
    and year=2021 and month=11 and day>=10
    and base_level>=16
) as tmp on tmp.role_id=major_users.uuid
) as a

left join
(
select  battle_id,mis_day,if(t2.role_id is null ,0,nums) as nums
from 
(
 select distinct battle_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day 
 from log_missions 
 where created_at>=1636934400 and created_at<1637539200 and base_level>=16
 and year=2021 and month=11 and day>=10
 and category=2
 ) as t1
left join 
( select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as troops_day ,sum(nums) as nums
    from log_troops
    where created_at>=1636934400 and created_at<1637539200 
    and action='6105' and nums>0
    and change_type in ('2','3') and year=2021 and month=11 and base_level>=16 and day>=10
    group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t2
on t1.battle_id=t2.role_id and t1.mis_day=t2.troops_day
) as b
on a.role_id=b.battle_id and a.login_day=b.mis_day
group by login_day


需求：死伤兵用户后续留存
需求目的：用于分析是否要调整死伤兵机制
筛选条件：11.15-11.21期间登录，且等级大于等于16级，唯一设备ID下最大等级角色
数据格式：日期，DAU，被攻击玩家数（当周有过登录），死伤兵为0的玩家数，流失用户数（一周内未登录）；
死兵小于1000的玩家数，流失用户数，
死兵1000-10000的玩家数，流失用户数，
死兵10000-50000的玩家数，流失用户数，
死兵5-10万玩家数，流失用户数，
死兵10-20万玩家数，流失用户数，
死兵20-30万玩家数，流失用户数
 @小乔 之前这份数据再追加一个流失用户数


with major_users as
(
  select uuid,server_id,reg_day,base_level,paid
  from
(
select uuid,server_id,paid,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where is_internal=false and server_id!='1'
 ) as a
inner join
 (select distinct role_id
    from log_login_role 
    where log_login_role.created_at>=1636934400 and log_login_role.created_at<1637539200
    
    ) as b on a.uuid=b.role_id
inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
        
    ) c on a.uuid=c.role_id and c.role_id=b.role_id

)

select login_day,count(distinct a.role_id) as dau,
    count(distinct b.battle_id) as "被攻击玩家数-活跃",
    count(distinct if(nums=0,battle_id,null)) as "死伤兵为0的玩家数",
    count(distinct if(nums=0,battle_id,null)) - count(distinct if(nums=0 and 
        date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵为0的流失用户数",
    count(distinct if(nums>0 and nums<=1000,battle_id,null)) as "死伤兵0-1000的玩家数",
    count(distinct if(nums>0 and nums<=1000,battle_id,null))-count(distinct if(nums>0 and nums<=1000 
        and  date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵0-1000的流失玩家数",
    count(distinct if(nums>1000 and nums<=10000,battle_id,null)) as "死伤兵1000-10000的玩家数",
    count(distinct if(nums>1000 and nums<=10000,battle_id,null))-count(distinct if(nums>1000 and nums<=10000
         and  date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵1000-10000的流失玩家数",
    count(distinct if(nums>10000 and nums<=50000,battle_id,null)) as "死伤兵10000-50000的玩家数",
    count(distinct if(nums>10000 and nums<=50000,battle_id,null))-count(distinct if(nums>10000 and nums<=50000
        and  date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵10000-50000的流失玩家数",
    count(distinct if(nums>50000 and nums<=100000,battle_id,null)) as "死伤兵50000-100000的玩家数",
    count(distinct if(nums>50000 and nums<=100000,battle_id,null))-count(distinct if(nums>50000 and nums<=100000
         and  date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵50000-100000的流失玩家数",
    count(distinct if(nums>100000 and nums<=200000,battle_id,null)) as "死伤兵100000-200000的玩家数",
    count(distinct if(nums>100000 and nums<=200000,battle_id,null))-count(distinct if(nums>100000 and nums<=200000
         and  date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵100000-200000的流失玩家数",
    count(distinct if(nums>200000 and nums<=300000,battle_id,null)) as "死伤兵200000-300000的玩家数",
    count(distinct if(nums>200000 and nums<=300000,battle_id,null))-count(distinct if(nums>200000 and nums<=300000
        and  date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵200000-300000的流失玩家数",
    count(distinct if(nums>300000,battle_id,null)) as "死伤兵300000+的玩家数",
    count(distinct if(nums>300000,battle_id,null))-count(distinct if(nums>300000 
         and  date_diff('day', date_parse(mis_day, '%Y-%m-%d'), date_parse(log_day, '%Y-%m-%d')) between 1 and 6,battle_id,null)) as "死伤兵300000+的流失玩家数"
from
(
select login_day,role_id
from major_users
inner join (
    select  distinct role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as login_day 
    from log_login_role 
    where log_login_role.created_at>=1636934400 and log_login_role.created_at<1637539200
    and year=2021 and month=11 and day>=10
    and base_level>=16
) as tmp on tmp.role_id=major_users.uuid
) as a

left join
(
select  battle_id,mis_day,if(t2.role_id is null ,0,nums) as nums
from 
(
 select distinct battle_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day 
 from log_missions 
 where created_at>=1636934400 and created_at<1637539200 and base_level>=16
 and year=2021 and month=11 and day>=10
 and category=2
 ) as t1
left join 
( select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as troops_day ,sum(nums) as nums
    from log_troops
    where created_at>=1636934400 and created_at<1637539200 
    and action='6105' and nums>0
    and change_type in ('2','3') and year=2021 and month=11 and base_level>=16 and day>=10
    group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t2
on t1.battle_id=t2.role_id and t1.mis_day=t2.troops_day
) as b
on a.role_id=b.battle_id and a.login_day=b.mis_day
left join (
 select distinct role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
 from log_login_role
 where month>=10
) as c  on a.role_id=c.role_id 

group by login_day



