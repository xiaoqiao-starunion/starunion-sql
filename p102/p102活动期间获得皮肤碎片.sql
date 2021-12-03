p102活动期间获得皮肤碎片.sql
活动日期	  当期皮肤	    活动期间累计获得对应皮肤碎片数满100的人数	 活动期间未购买当期通行证而获得当期皮肤人数(7100)	活动期间购买通行证人数(5523)
2021-08-03~2021-08-09	祈福猫灵	    206017
2021-08-10~2021-08-16	兰之誓约		206015	
2021-08-17~2021-08-23	磐石巨兽		206019	
2021-08-24~2021-08-30	骨下蔷薇		206021	
2021-08-31~2021-09-06	凤梨小屋		206022	
2021-09-07~2021-09-13	深海派对		206023	

1.购买通行证人数	
有通过【5523: 通行证付费奖励】，累计付费领取当期皮肤碎片＜100人数	
无【5523: 通行证付费奖励】领取记录且通过【7100:皮肤碎片兑换获得】获得当期皮肤碎片≥100人数

2.提数需求：各期DAU活动购买通行证玩家的估算等级
表结构：活动时间段，通行证玩家购买人数，估算最终达成等级1~20人数

select if(t2.item_id is null ,t1.item_id,t2.item_id) as item_id,count(distinct t2.role_id) as uv_pay, --购买通行证人数	
			
			count(distinct if(t2.nums<100 and t2.nums>0,t2.role_id,null)) as uv_pay_100_less,--付费且付费碎片小于100的玩家数
			count(distinct if(t2.role_id is null  and t1.nums>=100 ,t1.role_id,null)) as uv_only_free_and_100_more

from
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='7100' and --免费兑换
 created_at>=1627948800 and created_at<1628553600  --8.03-8.09
 and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t1 
full join
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='5523' and --付费兑换
 created_at>=1627948800 and created_at<1628553600  --8.03-8.09
 and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t2  on t1.role_id=t2.role_id
group by if(t2.item_id is null ,t1.item_id,t2.item_id)

union

select if(t2.item_id is null ,t1.item_id,t2.item_id) as item_id,count(distinct t2.role_id) as uv_pay, --购买通行证人数	
			
			count(distinct if(t2.nums<100 and t2.nums>0,t2.role_id,null)) as uv_pay_100_less,--付费且付费碎片小于100的玩家数
			count(distinct if(t2.role_id is null  and t1.nums>=100 ,t1.role_id,null)) as uv_only_free_and_100_more

from
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='7100' and --免费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
 created_at>=1628553600 and created_at<1629158400  --8.10-8.16
 and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t1 
full join
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='5523' and --付费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
 created_at>=1628553600 and created_at<1629158400  --8.10-8.16
 and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t2  on t1.role_id=t2.role_id
group by if(t2.item_id is null ,t1.item_id,t2.item_id)

union


select if(t2.item_id is null ,t1.item_id,t2.item_id) as item_id,count(distinct t2.role_id) as uv_pay, --购买通行证人数	
			
			count(distinct if(t2.nums<100 and t2.nums>0,t2.role_id,null)) as uv_pay_100_less,--付费且付费碎片小于100的玩家数
			count(distinct if(t2.role_id is null  and t1.nums>=100 ,t1.role_id,null)) as uv_only_free_and_100_more

from
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='7100' and --免费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
 created_at>=1629158400 and created_at<1629763200  --8.17-8.23
 and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t1 
full join
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='5523' and --付费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
 created_at>=1629158400 and created_at<1629763200  --8.17-8.23
 and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t2  on t1.role_id=t2.role_id
group by if(t2.item_id is null ,t1.item_id,t2.item_id)



union


select if(t2.item_id is null ,t1.item_id,t2.item_id) as item_id,count(distinct t2.role_id) as uv_pay, --购买通行证人数	
			
			count(distinct if(t2.nums<100 and t2.nums>0,t2.role_id,null)) as uv_pay_100_less,--付费且付费碎片小于100的玩家数
			count(distinct if(t2.role_id is null  and t1.nums>=100 ,t1.role_id,null)) as uv_only_free_and_100_more

from
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='7100' and --免费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
 created_at>=1629763200 and created_at<1630368000  --8.24-8.30
 and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t1 
full join
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='5523' and --付费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
 created_at>=1629763200 and created_at<1630368000  --8.24-8.30
 and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t2  on t1.role_id=t2.role_id
group by if(t2.item_id is null ,t1.item_id,t2.item_id)

union


select if(t2.item_id is null ,t1.item_id,t2.item_id) as item_id,count(distinct t2.role_id) as uv_pay, --购买通行证人数	
			
			count(distinct if(t2.nums<100 and t2.nums>0,t2.role_id,null)) as uv_pay_100_less,--付费且付费碎片小于100的玩家数
			count(distinct if(t2.role_id is null  and t1.nums>=100 ,t1.role_id,null)) as uv_only_free_and_100_more

from
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='7100' and --免费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
 created_at>=1630368000 and created_at<1630972800  --8.31-9.06
 and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t1 
full join
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='5523' and --付费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
 created_at>=1630368000 and created_at<1630972800  --8.31-9.06
 and item_id = 206022
-- created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
-- and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t2  on t1.role_id=t2.role_id
group by if(t2.item_id is null ,t1.item_id,t2.item_id)

union


select if(t2.item_id is null ,t1.item_id,t2.item_id) as item_id,count(distinct t2.role_id) as uv_pay, --购买通行证人数	
			
			count(distinct if(t2.nums<100 and t2.nums>0,t2.role_id,null)) as uv_pay_100_less,--付费且付费碎片小于100的玩家数
			count(distinct if(t2.role_id is null  and t1.nums>=100 ,t1.role_id,null)) as uv_only_free_and_100_more

from
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='7100' and --免费兑换
 --created_at>=1627948800 and created_at<1628553600  --8.03-8.09
 --and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
 created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
 and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t1 
full join
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='5523' and --付费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
 created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
 and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t2  on t1.role_id=t2.role_id
group by if(t2.item_id is null ,t1.item_id,t2.item_id)





2.提数需求：各期DAU活动购买通行证玩家的估算等级
表结构：活动时间段，通行证玩家购买人数，估算最终达成等级1~20人数

select   t1.item_id,type,
		count(distinct t2.role_id) as finish_uv --任务完成
from
(
select role_id,item_id,sum(nums) as nums,action
from log_item
where nums>0 and action='5523' and --付费兑换
-- created_at>=1627948800 and created_at<1628553600  --8.03-8.09
-- and item_id = 206017
-- created_at>=1628553600 and created_at<1629158400  --8.10-8.16
-- and item_id = 206015
-- created_at>=1629158400 and created_at<1629763200  --8.17-8.23
-- and item_id = 206019
-- created_at>=1629763200 and created_at<1630368000  --8.24-8.30
-- and item_id = 206021
-- created_at>=1630368000 and created_at<1630972800  --8.31-9.06
-- and item_id = 206022
 created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
 and item_id = 206023
and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by role_id,item_id,action
) as t1
left join
(
select role_id ,nums,
            case when nums>= 0 and nums< 10000 then '1' 
                 when nums>= 10000 and nums< 20500 then '2' 
                  when nums>= 20500 and nums< 31500 then '3' 
                  when nums>= 31500 and nums< 43500 then '4' 
                 when nums>= 43500 and nums< 56000 then '5' 
                 when nums>= 56000 and nums< 69000 then '6' 
                when nums>= 69000 and nums< 83000 then '7' 
                 when nums>= 83000 and nums< 98000 then '8' 
                  when nums>= 98000 and nums< 113500 then '9' 
                 when nums>= 113500 and nums< 130000 then '10' 
                 when nums>= 130000 and nums< 147500 then '11' 
                 when nums>= 147500 and nums< 166000 then '12' 
                  when nums>= 166000 and nums< 185500 then '13' 
                  when nums>= 185500 and nums< 206000 then '14' 
                  when nums>= 206000 and nums< 228000 then '15' 
                   when nums>= 228000 and nums< 251000 then '16' 
                   when nums>= 251000 and nums< 275500 then '17' 
                   when nums>= 275500 and nums< 301000 then '18' 
                    when nums>= 301000 and nums< 328000 then '19' 
                when nums>= 328000 then '20' end
              as type     
from (
select role_id,170*(count(role_id)) nums 
from log_item 
 
--where created_at>=1627948800 and created_at<1628553600  --8.03-8.09
--and item_id = 204127 --骨头碎片消耗
--where created_at>=1628553600 and created_at<1629158400  --8.10-8.16
--and item_id = 204124 --植物叶片消耗
--where created_at>=1629158400 and created_at<1629763200  --8.17-8.23
--and item_id = 204126 --小块砂石消耗
--where created_at>=1629763200 and created_at<1630368000  --8.24-8.30
--and item_id = 204127 --骨头碎片消耗
--where created_at>=1630368000 and created_at<1630972800  --8.31-9.06
--and item_id = 204124 --植物叶片消耗
where created_at>=1630972800 and  created_at<1631577600 --09-07~09-13
and item_id = 204125 --贝壳碎片消耗

and nums <0
group by role_id 
)a 
)as t2  on t1.role_id=t2.role_id

group by t1.item_id,type

