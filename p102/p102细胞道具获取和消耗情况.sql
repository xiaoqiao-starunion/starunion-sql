需求：细胞培养情况
需求目的：分析更新后培养行为是否上升
筛选条件：9.28-10.22
数据结构1：日期，参与人数(所有有消耗记录的玩家去重），
	细胞消耗途径，消耗数量，对应人数，
	细胞液I-IV消耗途径，消耗数量，对应人数；
	小细胞核消耗途径，消耗数量，对应人数；
	大细胞核消耗途径，消耗数量，对应人数；
	生命精华消耗途径，消耗数量，对应人数
数据结构2：日期，参与人数(所有有获取记录的玩家去重），
	细胞获取途径，获取数量，对应人数，
	细胞液I-IV获取途径，获取数量，对应人数；
	小细胞核获取途径，获取数量，对应人数；
	大细胞核获取途径，获取数量，对应人数；
	生命精华获取途径，获取数量，对应人数
数据结构3：日期，细胞ID，获取途径，获取人数


--消耗
select t1.daily,t2.uv_all,item_id,action,uv1,nums
from(
select 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily,
	action,item_id,sum(nums) as nums,
	count(distinct role_id) as uv1

from log_item
where nums<0
	and item_id between 204154 and 204160 --细胞液\大小细胞核生命精华
	and created_at>=1632787200 and created_at<1634947200
	and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by 
	action,item_id,
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t1
inner join
( 
select 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily,
	count(distinct role_id) as uv_all

from log_item
where nums<0
	and item_id between 204154 and 204160 --细胞液\大小细胞核生命精华
	and created_at>=1632787200 and created_at<1634947200
	and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t2 on t1.daily=t2.daily


--获得
select t1.daily,t2.uv_all,item_id,action,uv1,nums
from(
select 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily,
	action,item_id,sum(nums) as nums,
	count(distinct role_id) as uv1

from log_item
where nums>0
	and (item_id between 204154 and 204160 --细胞液\大小细胞核生命精华
	or item_id between 210001 and 210042) --细胞
	and created_at>=1632787200 and created_at<1634947200
	and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by 
	action,item_id,
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t1
inner join
( 
select 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily,
	count(distinct role_id) as uv_all

from log_item
where nums>0
	and (item_id between 204154 and 204160 --细胞液\大小细胞核生命精华
	or item_id between 210001 and 210042) --细胞
	and created_at>=1632787200 and created_at<1634947200
	and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as t2 on t1.daily=t2.daily




select 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily,
	action,
	item_id,
	count(distinct role_id) as uv_all
from log_item
where nums>0
	and item_id between 210001 and 210042 --细胞
	and created_at>=1632787200 and created_at<1634947200
	and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by 
	DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )as daily,
	action,
	item_id