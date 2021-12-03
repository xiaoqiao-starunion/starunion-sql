浆果大丰收获取幽灵菇的玩家基础信息

需求目的：消耗幽灵菇的玩家储值信息
条件：11/2-11/6 (UTC)， S1-S452，玩家唯一ID,幽灵菇消耗，幽灵菇获取，幽灵菇内购，玩家储值（11.6 23:59时）
输出：

玩家ID、获取幽灵菇、消耗幽灵菇、礼包获得幽灵菇、玩家总充值


礼包ID	礼包	礼包单价($)	幽灵菇
2930201	万圣节特供：特级异化卵4.99	4.99	3
2930401	万圣节特供：橙色野怪自选4.99	4.99	3
2930001	万圣节资源(合计:11.4M)4.99	4.99	3
2930202	万圣节特供：特级异化卵9.99	9.99	6
2930402	万圣节特供：橙色野怪自选9.99	9.99	6
2930002	万圣节资源(合计:19.4M)9.99	9.99	6
2930003	万圣节资源(合计:38M)19.99	19.99	11
2930203	万圣节特供：特级异化卵19.99	19.99	11
2930403	万圣节特供：橙色野怪自选19.99	19.99	11
2930004	万圣节资源(合计:53M)49.99	49.99	20
2930404	万圣节特供：橙色野怪自选49.99	49.99	20
2930204	万圣节特供：特级异化卵49.99	49.99	20
2930501	万圣节限定：橙色野怪自选99.99	99.99	40
2930301	万圣节限定：特级异化卵99.99	99.99	40
2930101	万圣节特惠:资源(合计:97M)99.99	99.99	40
29308	万圣节：每日奖励	0	1


select t1.role_id,use_nums,get_nums_free,get_nums_free_2,get_nums_pay,pay_total
from
(
	select role_id,sum(nums) as use_nums
	from log_item
	where item_id=204178
	and nums<0
	and role_id in (select uuid from roles where is_internal=false)
	and try_cast(server_id as int) between 2 and 452
	and created_at>=1635811200 and created_at<1636243200
	and month = 11
	and action='5561'
	group by role_id
) as t1
left join
(	select role_id,sum(nums) as get_nums_free --道具表记录购买礼包获取的情况 用的是内购
	from log_item
	where created_at>=1635811200 and created_at<1636243200 
	and nums>0 and item_id=204178
	and action='5562' --代币领取
 group by role_id
)as t2
on t1.role_id=t2.role_id
left join
(	select role_id,sum(nums) as get_nums_free_2 --道具表记录购买礼包获取的情况 用的是内购
	from log_item
	where created_at>=1635811200 and created_at<1636243200 
	and nums>0 and item_id=204178
	and action='5525' --免费礼包获得
 group by role_id
)as t5
on t1.role_id=t5.role_id
left join
(		select role_id,sum(nums) as get_nums_pay --道具表记录购买礼包获取的情况 用的是内购
	from log_item
	where created_at>=1635811200 and created_at<1636243200 
	and nums>0 and item_id=204178
	and action='3300' --付费礼包
 group by role_id
)as t3
on t1.role_id=t3.role_id
left join
( select role_id,sum(price) as pay_total
	from payments
	where  is_paid=1
	and is_test=2
	and status=2
	and created_at<1636243200
	group by role_id

)as t4
on t1.role_id=t4.role_id
