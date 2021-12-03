需求：VIP系统使用情况
需求目的：分析参与情况
筛选条件1：10.18-10.21登录，VIP等级>1
数据结构1：玩家ID，区服，注册时间，等级，累计充值金额，VIP等级



筛选条件2：10.18-10.21期间有过签到记录和领取每日奖励的玩家
数据结构2：日期，DAU，VIP等级大于1的DAU，签到人数，每日奖励领取人数9024
 @小乔 

 select uuid,roles.server_id,base_level,
        DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) reg_day,
        paid,t1.vip_level
 from roles
inner join
(
 select role_id,row_number() over(partition by role_id order by created_at desc) as rank, vip_level
 from log_vip
 where vip_level>1
 and created_at>=1634515200 and created_at<1634860800
 ) as t1 on t1.role_id=roles.uuid
inner join (
 select distinct role_id
 from log_login_role 
 where created_at>=1634515200 and created_at<1634860800
) as t2
on t1.role_id=t2.role_id and roles.uuid=t2.role_id
where rank=1 
and is_internal=false and roles.server_id!='1'





 select t1.daily,dau,uv1,uv2,uv3
 from  (
 select count(distinct role_id) as dau,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily
 from log_login_role 
 where created_at>=1634515200 and created_at<1634860800
 and role_id in (select uuid from roles where is_internal=false and roles.server_id!='1')
 and month>=10
 group by DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as t1
 left join
 (select count(distinct tmp1.role_id) as uv1,tmp1.daily
 from(
    select role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily,
        row_number() over(partition by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
        order by created_at desc) as rank, vip_level
    from log_vip
    where vip_level>1
    and created_at>=1634515200 and created_at<1634860800
    and month>=10
    and role_id in (select uuid from roles where is_internal=false and roles.server_id!='1')
 ) as tmp1
 inner join (
     select distinct role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
     from log_login_role 
     where created_at>=1634515200 and created_at<1634860800
     and month>=10
 ) as tmp2 on tmp1.role_id=tmp2.role_id and tmp1.daily=tmp2.log_day
 where rank=1 and vip_level>1
 group by tmp1.daily
)as t2 on t1.daily=t2.daily
 left join
 ( select count(distinct split(split(role_id,',')[1],':')[2]) as uv2,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily
    from log_game_step
    where step_type='vip_sign_in'
     and created_at>=1634515200 and created_at<1634860800
    and month>=10
    and split(split(role_id,',')[1],':')[2] in (select uuid from roles where is_internal=false and roles.server_id!='1')
    group by DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t3 on t1.daily=t3.daily
 left join
 (select count(distinct role_id) as uv3,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily
    from log_activity_award
    where action='9024'
     and created_at>=1634515200 and created_at<1634860800
    and month>=10
    and role_id in (select uuid from roles where is_internal=false and roles.server_id!='1')
    group by DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as t4 on t1.daily=t4.daily




