用户注册后4-5个月的登录天数.sql 20210927
需求：用户注册4-5个月的活跃行为
需求目的：老用户连续登录情况
筛选条件：2-103服，9.6-9.10迁服后的玩家归属到原服务器
数据格式1：  
玩家ID，等级，国家，服务器，注册时间，注册第4个月登录天数，注册第5个月登录天数
 @小乔 


with users_base as
(
select role_id,server_id,reg_day,country,base_level,last_login_day
from
(
    
        select uuid as role_id,reg_day,base_level,last_login_day
                ,if(last_server_id is not null,last_server_id,server_id) as server_id,country,created_at,account_id
        from
        (
        select uuid,server_id,country,created_at,account_id -- 每个用户的最新ID
                ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) as reg_day
                ,base_level
                ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) last_login_day
        from roles
        where is_internal = false and server_id!='1'
        ) as t1
        left join
        (
        select  role_id,last_server_id,server_id as new_server,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%I:%S' ) transfer_date
            from log_fly
            where fly_category=204147 and year=2021 and month=9
            and created_at >=1630886400 and  created_at <1631318400

        ) as t2 on t1.uuid=t2.role_id
)as tmp where try_cast(server_id as int) between 2 and 103
)



   select role_id,server_id,reg_day,country,base_level,last_login_day,
     count(distinct if(diff between 91 and 120,login_day,null)) as log_days_4,
     count(distinct if(diff between 121 and 150,login_day,null)) as log_days_5
    from
    (
        select role_id,server_id,reg_day,country,login_day,base_level,last_login_day,
        date_diff('day',date_parse(reg_day,'%Y-%m-%d'),date_parse(login_day,'%Y-%m-%d')) as diff
        from
        (
        select users_base.role_id,server_id,reg_day,login_day,country,base_level,last_login_day
        from users_base
        left join 
        (select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) login_day
        from log_login_role
        where year=2021 
        group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
        )as a on a.role_id=users_base.role_id
        ) as b
        where date_diff('day',date_parse(reg_day,'%Y-%m-%d'),date_parse(login_day,'%Y-%m-%d')) between 91 and 150
    ) as tmp1 
     group by role_id,server_id,reg_day,country,base_level,last_login_day
   

