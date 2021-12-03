每日特级孵化参与情况 英雄招募
log_item: 包括1.免费抽：获得道具、英雄  2.消耗特级异化卵抽 （使用招募英雄的action&获得,算参与人数）
log_hero:只记录抽到英雄的情况
日期，DAU（按设备ID取，注册3天以上，等级大于等于8级）,参与人数去重
 @小乔 

with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day,paid
        from
(
select uuid,server_id,paid
        ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where created_at < 1631664000 --注册时间<9.15

    and base_level>=8
    and is_internal=false and server_id!='1'

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


select  log_day,dau,uv
from(
	select count(distinct role_id) as dau,log_day
	from
		(select  distinct role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day
	from log_login_role inner join major_users on major_users.uuid=log_login_role.role_id
	where month>=3
	) as tmp group by log_day
)as t1

left join
(  select count(distinct role_id) as uv ,act_day
	from
	(
	select role_id, nums,act_day
	from
	(
	SELECT role_id, sum(nums) as nums,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as act_day
	FROM log_item inner join major_users on major_users.uuid=log_item.role_id
	where action in ('2801','2800','8008') 
	group by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
	) as t 
	) as tmp group by act_day
) as t2 on  log_day=act_day
