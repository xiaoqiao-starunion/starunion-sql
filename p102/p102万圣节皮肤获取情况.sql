提数需求
1.需求：万圣节皮肤获取情况
2.需求目的：免费玩家拿完整万圣节皮肤情况
3.筛选条件
服务器 S2-S452 
既通过 【5522: 通行证免费奖励】 和 【4501: 节日签到奖励】 获取 【206037: 暗卉巫灵皮肤碎片】 数量
4.数据结构
玩家id，【去重】， 服务器 ，历史充值金额 ，【5522: 通行证免费奖励】 ，【4501: 节日签到奖励】，【7103: 兑换皮肤道具扣除】

select uuid,server_id,paid,nums_5522,nums_4501,nums_1400,nums_7103
from roles
left join
(select role_id,action,sum(nums) as nums_5522
	from log_item
	where action in ('5522')
	and item_id=206037 
	and nums>0
	group by role_id,action
)as t1 
on roles.uuid=t1.role_id
left join
(
	select role_id,action,sum(nums) as nums_4501
	from log_item
	where action in ('4501')
	and item_id=206037 
	and nums>0
	group by role_id,action
)as t2
on roles.uuid=t2.role_id
left join
(
	select role_id,action,sum(nums) as nums_1400
	from log_item
	where action in ('1400')
	and item_id=206037 
	and nums>0
	group by role_id,action
)as t4 on roles.uuid=t4.role_id
left join 
(select role_id,action,sum(nums) as nums_7103
	from log_item
	where action in ('7103')
	and item_id=206037 
	and nums<0
	group by role_id,action

)as t3 
on roles.uuid=t3.role_id 

where is_internal=false and try_cast(server_id as int) between 2 and 452 
and ( t2.role_id is not null)