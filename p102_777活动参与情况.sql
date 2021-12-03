
2.需求：777活动参与情况
目的：了解777活动期间玩家参与及付费情况
筛选：2021.11.21-2021.11.25
数据需求
1.角色id 枫叶消耗量 枫叶获得量-任务(5562?) 枫叶获得量-免费礼包(5525) 枫叶获得量-付费礼包(3300) 历史充值金额 活动付费金额
活动付费金额：活动期间购买活动相关礼包总金额
29321: 感恩节资源 				id:2932101 
29321: 感恩节资源 				id:2932102 
29321: 感恩节资源 				id:2932103 
29321: 感恩节资源 				id:2932104 
29322: 感恩节特惠:资源 			id:2932201 
29323: 感恩节特供：特级异化卵 		id:2932301 
29323: 感恩节特供：特级异化卵 		id:2932302 
29323: 感恩节特供：特级异化卵 		id:2932303 
29323: 感恩节特供：特级异化卵 		id:2932304 
29324: 感恩节限定：特级异化卵 		id:2932401 
29325: 感恩节特供：橙色野怪自选 	id:2932501 
29325: 感恩节特供：橙色野怪自选 	id:2932502 
29325: 感恩节特供：橙色野怪自选 	id:2932503 
29325: 感恩节特供：橙色野怪自选 	id:2932504 
29326: 感恩节限定：橙色野怪自选 	id:2932601 


select uuid,paid,use_nums,get_5562,get_5525,get_3300,pay_activity
from roles 
left join
(
	select role_id,sum(nums) as use_nums --消耗量
	from log_item
	where month=11 and created_at>=1637452800 and created_at<1637884800
	and item_id=204182 --枫叶
	and nums<0 
	group by role_id
)as t1 on roles.uuid=t1.role_id
left join 
( select role_id,
	sum(if(action='5562',nums,0))as get_5562,
	sum(if(action='5525',nums,0))as get_5525,
	sum(if(action='3300',nums,0))as get_3300
	from log_item
	where month=11 and created_at>=1637452800 and created_at<1637884800
	and item_id=204182 --枫叶
	group by role_id
)as t2 on roles.uuid=t2.role_id
left join 
( select role_id,sum(price) as pay_activity
	from payments
	where created_at>=1637452800 and created_at<1637884800
	and is_paid=1 and is_test=2 and status=2
	and conf_id in (2932101,2932102,2932103,2932104,2932201,2932301,2932302,
		2932303,2932304,2932401,2932501,2932502,2932503,2932504,2932601)
	group by role_id
)as t3 on roles.uuid=t3.role_id
where  is_internal=false and server_id!='1' and (use_nums is not null or get_3300 is not null or get_5525 is not null or get_5562 is not null)