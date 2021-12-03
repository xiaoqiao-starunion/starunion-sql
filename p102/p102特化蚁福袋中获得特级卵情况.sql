特化蚁福袋中获得特级卵情况

时间范围：9月13日至今
道具：217022: 特化蚁成长福袋 204023: 特级异化卵
输出：分日特化蚁成长福袋获取量，分日特化蚁成长福袋消耗量，
分日通过217022: 特化蚁成长福袋获得的特级异化卵量（可能和星球馈赠的箱子用了一个，能区分最好区分） @小乔 


特化蚁成长福袋获得的特级异化卵量 时间相等
select sum(nums) as nums ,count(distinct role_id),DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as get_date
from
(
select t1.role_id,t1.nums,t1.created_at
from
(select item_id,role_id,action,nums,created_at,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d %H:%I:%S') as get_time
from log_item
where action='3900' and nums>0 and item_id=204023
 and month>=9 and day>=12
)as t1
 inner join
 (
select item_id,role_id,action,created_at,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d %H:%I:%S') as open_time
from log_item
where item_id=217022 and nums<0
   and month>=9 and day>=12
) as t2 on t1.role_id=t2.role_id 
 and t2.created_at=t1.created_at
) as tmp group by DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')



select open_day,use_nums,use_uv,get_nums,get_uv
from
(select open_day,sum(nums) as use_nums,count(distinct role_id) as use_uv
from
(
	select item_id,role_id,action,nums,
	DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as open_day
	from log_item
	where item_id=217022 and nums<0
	   and month>=9 and day>=12
)as a group by open_day
)as t1
inner join 
( select get_day,sum(nums) as get_nums,count(distinct role_id) as get_uv
from(
	select item_id,role_id,action,nums,
	DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as get_day
	from log_item
	where item_id=217022 and nums>0
	   and month>=9 and day>=12
	) as b group by get_day
)as t2
on open_day=get_day

