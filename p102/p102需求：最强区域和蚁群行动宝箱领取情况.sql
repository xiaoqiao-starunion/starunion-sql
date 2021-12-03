需求：最强区域和蚁群行动宝箱领取情况
需求目的：分析领奖产出情况
筛选条件：筛选时间（9.28-10.7），按设备ID，看不同等级下的宝箱领取情况
数据结构：
日期，玩家等级（等级），宝箱档位，领取人数

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
    where created_at<1633651200 and created_at >= 1632787200  --（9.28-10.7） 活跃
 and year=2021 and month>=9 
) as c on a.uuid=c.role_id
)

select ac_id,act_day,base_level,award_node,extend_1,count(distinct role_id) as uv
from
(select ac_id,role_id,log_activity_award.base_level,award_node,extend_1,
	DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day 
from log_activity_award inner join major_users
on major_users.uuid=log_activity_award.role_id
where ac_id in ('1000101' , '1000300')
  and  created_at<1633651200 and created_at >= 1632787200  --（9.28-10.7） 活跃
  and year=2021 and month>=9
) as t1 group by ac_id,act_day,base_level,award_node,extend_1