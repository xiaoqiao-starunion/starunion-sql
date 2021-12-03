提数需求
1.需求：玩家野怪的培养情况
2.需求目的：分析玩家野怪的培养情况
3.筛选条件
10.18~10.24登陆过的玩家，拥有野怪孵化地和野怪栖息地，唯一设备id
4.数据结构
玩家id 服务器 注册时间 等级 历史充值总额 野怪id 野怪品质 野怪类型 星级 是否上阵或工作
 @小乔  


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
	where log_login_role.created_at>=1634515200 and log_login_role.created_at<1635120000
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


select role_id,server_id,reg_day,base_level,paid,hero_id,hero_type,level,amount
from
(
select a.role_id,server_id,reg_day,base_level,paid,id,level,amount
from
	(
	select role_id,t1.id,t1.level,t1.amount
	from 
	(select role_id,params,row_number() over(partition by role_id order by created_at desc) as rank
	from log_role_data
	where type=2 and month=10
	and created_at<1635120000
	) as t0
	cross join unnest(cast(cast(json_parse(params) as array(json)) as array(row(Id INTEGER,level INTEGER,amount INTEGER)))) 
	as temp(t1)
	where rank=1
	) as a
	inner join
	( select uuid,server_id,reg_day,base_level,paid
	from major_users
	inner join 
	( select distinct role_id
		from log_buildings
		where build_conf_id between 4301000 and 4302025
		and action in ('6202','6201')
		and created_at<1635120000
	) as tmp1 on tmp1.role_id=uuid
	) as b on a.role_id=b.uuid
) as t1
inner join
( select distinct hero_id,hero_type
	from log_hero
	where month>=9
) as t2
on t1.id=t2.hero_id
