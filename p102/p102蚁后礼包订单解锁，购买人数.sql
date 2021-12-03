2.蚁后礼包
需求：蚁后礼包订单解锁，购买人数
目的：了解蚁后礼包解锁->创建订单->购买的转换率
筛选：
日期：05.18-11.14
订单id：
2909701	蚁巢-等级限定礼包4.99
2909801	蚁巢-等级限定礼包9.99
2909901	蚁巢-等级限定礼包19.99
2910001	蚁巢-等级限定礼包49.99
玩家类型分：未充值，小R，中R，大R
服务器批次：
第一批：S2-S112
第二批：S113-S430
数据需求：
1.服务器批次 礼包名称 玩家类型 首次解锁人数	二次解锁人数
2.服务器批次 礼包名称 玩家类型 订单创建人数 购买人数 订单创建时平均等级 订单创建时等级中位数 购买时平均等级 购买时等级中位数
备注：
礼包解锁条件
蚁巢-等级限定礼包4.99： 如果玩家8級彈出來沒賣    那就10級在彈出來（时长：48h，错过就无）
蚁巢-等级限定礼包9.99： 如果玩家11級彈出來沒賣   那就13級在彈出來（时长：48h，错过就无）
蚁巢-等级限定礼包19.99：如果玩家15級彈出來沒賣   那就17級在彈出來（时长：48h，错过就无）
蚁巢-等级限定礼包49.99：如果玩家19級彈出來沒賣   那就22級在彈出來（时长：48h，错过就无）

select servers_type,user_type,count(distinct if(base_level>=8,uuid,null)) as level_8_more
	,count(distinct if(base_level >=10,uuid,null)) level_10_more
	,count(distinct if(base_level >=11,uuid,null)) level_11_more
	,count(distinct if(base_level >=13,uuid,null)) level_13_more
	,count(distinct if(base_level >=15,uuid,null)) level_15_more
	,count(distinct if(base_level >=17,uuid,null)) level_17_more
	,count(distinct if(base_level >=19,uuid,null)) level_19_more
	,count(distinct if(base_level >=22,uuid,null)) level_22_more
from
(
select uuid,case when try_cast(server_id as int) between 2 and 112 then 'servers_1'
				when try_cast(server_id as int) between 113 and 430 then 'servers_2'
				end as servers_type,
			case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false 
) as t1 inner join 
( select role_id,base_level,row_number() over(partition by role_id order by created_at desc) as rank
	from log_login_role
	where created_at<1636934400
) as t2 on t1.uuid=t2.role_id
where rank=1 and user_type is not null and servers_type is not null
group by servers_type,user_type





select servers_type,user_type,t2.conf_id,count(distinct t2.role_id) as created_uv,count(distinct t3.role_id) as pay_uv
from
(
select uuid,case when try_cast(server_id as int) between 2 and 112 then 'servers_1'
				when try_cast(server_id as int) between 113 and 430 then 'servers_2'
				end as servers_type,
			case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false 
) as t1 inner join 
( select role_id,base_level,conf_id
	from payments
	where created_at<1636934400 and created_at>=1621296000 
	and conf_id in (2909701,2909801,2909901,2910001)
) as t2 on t1.uuid=t2.role_id
left join 
( select role_id,base_level,conf_id
	from payments
	where created_at<1636934400 and created_at>=1621296000 
	and conf_id in (2909701,2909801,2909901,2910001)
	and is_paid=1 and is_test=2 and status=2
) as t3 on t1.uuid=t3.role_id and t2.conf_id=t3.conf_id
where user_type is not null and servers_type is not null
group by servers_type,user_type,t2.conf_id




select servers_type,user_type,t2.conf_id,(sum(t2.base_level)*1.0)/(count( t2.role_id)*1.0) as per_level_created
from
(
select uuid,case when try_cast(server_id as int) between 2 and 112 then 'servers_1'
				when try_cast(server_id as int) between 113 and 430 then 'servers_2'
				end as servers_type,
			case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false 
) as t1 inner join 
( select role_id,base_level,conf_id
	from payments
	where created_at<1636934400 and created_at>=1621296000 
	and conf_id in (2909701,2909801,2909901,2910001)
) as t2 on t1.uuid=t2.role_id
group by servers_type,user_type,t2.conf_id



select servers_type,user_type,t3.conf_id,
(sum(t3.base_level)*1.0)/(count( t3.role_id)*1.0) as per_level_pay
from
(
select uuid,case when try_cast(server_id as int) between 2 and 112 then 'servers_1'
				when try_cast(server_id as int) between 113 and 430 then 'servers_2'
				end as servers_type,
			case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false 
) as t1 inner join 
( select role_id,base_level,conf_id
	from payments
	where created_at<1636934400 and created_at>=1621296000 
	and conf_id in (2909701,2909801,2909901,2910001)
	and is_paid=1 and is_test=2 and status=2
) as t3 on t1.uuid=t3.role_id 
where user_type is not null and servers_type is not null
group by servers_type,user_type,t3.conf_id


select servers_type,user_type,t2.conf_id,t2.base_level as created_level, t2.role_id as created_uid
from
(
select uuid,case when try_cast(server_id as int) between 2 and 112 then 'servers_1'
				when try_cast(server_id as int) between 113 and 430 then 'servers_2'
				end as servers_type,
			case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false 
) as t1 inner join 
( select role_id,base_level,conf_id
	from payments
	where created_at<1636934400 and created_at>=1621296000 
	and conf_id in (2909701,2909801,2909901,2910001)
) as t2 on t1.uuid=t2.role_id


select servers_type,user_type,t3.conf_id, t3.base_level as pay_level,t3.role_id as pay_uid
from
(
select uuid,case when try_cast(server_id as int) between 2 and 112 then 'servers_1'
				when try_cast(server_id as int) between 113 and 430 then 'servers_2'
				end as servers_type,
			case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false 
) as t1 inner join 
( select role_id,base_level,conf_id
	from payments
	where created_at<1636934400 and created_at>=1621296000 
	and conf_id in (2909701,2909801,2909901,2910001)
	and is_paid=1 and is_test=2 and status=2
) as t3 on t1.uuid=t3.role_id 
where user_type is not null and servers_type is not null
