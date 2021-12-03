需求：VIP商店道具购买与钻石消耗
需求目的：分析商店首开道具售卖效果
筛选条件：UTC 10月23号00:00~10:00，行为ID=9026，道具获取详情和钻石变动详情
数据结构1：道具ID，获取数量

数据结构2：玩家ID，服务器，等级，vip商店的钻石消耗量，截止10点的钻石剩余量，历史总充值金额

select item_id,sum(nums) as nums
from log_item
where action='9026' and nums>0
 and created_at>=1634947200 and created_at<1634983200
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
 group by item_id





select t1.role_id,t1.server_id,t1.base_level,paid,vip_nums,balance
from
(
 select role_id,roles.server_id,roles.base_level,paid,consume_item_id,sum(consume_nums) as vip_nums
from log_item_exchange 
  inner join roles on roles.uuid=log_item_exchange.role_id
where ac_id='9026' and action='gem'
    and log_item_exchange.created_at>=1634947200 and log_item_exchange.created_at<1634983200
    and is_internal=false and roles.server_id!='1'
  group by role_id,roles.server_id,roles.base_level,consume_item_id,paid
) as t1
inner join
( 
select role_id,
	sum(balance) as balance
from
(
	select role_id,
	sum(balance) as balance
	from
	(	select role_id,item_id,
		case when item_id=200073 then balance*5
				when item_id=200074 then balance*10
				when item_id=200075 then balance*20
				when item_id=200076 then balance*50
				when item_id=200077 then balance*100
				when item_id=200078 then balance*200
				when item_id=200079 then balance*1000
				when item_id=200080 then balance*5000
				end as balance
	from
	(
	SELECT role_id,item_id,balance,
			
			row_number() over(partition by role_id,item_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id between 200073 and 200080
	and created_at<1634983200
	) as t where rank=1  
	) as tmp group by role_id

union all

	select  role_id,balance
	from
	(
	SELECT role_id,resource_id,balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_resource 
	where resource_id =1 and created_at<1634983200
	) as t where rank=1  
) as tmp group by role_id

) as t2 on t1.role_id=t2.role_id

