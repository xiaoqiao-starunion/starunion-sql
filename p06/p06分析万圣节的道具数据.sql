@动人  大佬麻烦两个提数需求，感谢
1、需求
在道具表中通过备注“301”筛选出感恩节活动中玩家战令达到30级的玩家，然后再查看这些玩家的火鸡过期道具和幸运骨过期道具的数据（itemid：10330002、10330004）
2、目的
分析万圣节的道具过期数据
3、筛选条件
时间段：UTC 2021.11.26 0:00后（道具在这个时间自动转化为过期道具）
服务器：S1-S17
4、数据格式


select t1.role_id,roles.server_id,roles.base_level,item_id, nums
from roles
inner join
(
select distinct role_id
from log_item
where extend_1='301' 
) as t1 on roles.uuid=t1.role_id
left join
( select role_id,item_id,sum(nums) as nums
from log_item
where item_id in (10330002,10330004) and action='1205'
and created_at>=1637884800
group by item_id,role_id
)as t2 on roles.uuid=t2.role_id and t1.role_id=t2.role_id
where is_internal=false 




1、需求
在道具表中通过备注“301”筛选出感恩节活动中玩家战令达到30级的玩家，然后查看这些玩家整个活动期间两个活动道具的购买数量（itemid：10330001、10330003），以及活动期间的付费总额
2、目的
分析万圣节的道具购买数据
3、筛选条件
时间段：UTC 2021.11.19 0:00 - 2021.11.25 23:59
服务器：S1-S17


select t1.role_id,roles.server_id,roles.base_level,pay,item_id, nums
from roles
inner join
(
select distinct role_id
from log_item
where extend_1='301' 
) as t1 on roles.uuid=t1.role_id
left join
( select role_id,item_id,sum(nums) as nums
from log_item
where item_id in (10330001,10330003) and action='1601'
and created_at<1637884800 and created_at>=1637280000 
group by item_id,role_id
)as t2 on roles.uuid=t2.role_id and t1.role_id=t2.role_id
left join (
select role_id,sum(price) as pay
from payments
where is_paid=1 and is_test=2 and status=2 and created_at<1637884800 and created_at>=1637280000 
group by role_id
)as t3 on roles.uuid=t3.role_id and t1.role_id=t3.role_id
where is_internal=false 

