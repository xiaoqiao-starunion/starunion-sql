提数需求
1.需求：最强区域玩家得分
2.需求目的：分析最强区域玩家活动参与情况
3.筛选条件
日期:9.27-10.10 服务器:2-356服 玩家得分大于等于380000分
4.数据结构(csv格式文件)
用户id 等级 日期 星期 服务器 最强区域玩家得分
每天第一次参与时的等级，确定当天的各档位划分。不会因为当天等级变化，档位划分发生变化。
 @小乔 
select t1.role_id,base_level,t1.daily,t1.server_id,nums
from
(
select role_id,server_id,daily,nums
from
(
 select role_id,server_id,sum(nums) as nums,
        DATE_FORMAT( FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d' ) as daily
       
 from log_activity_score
 where ac_id='1000300'
 and created_at<1633910400
 and created_at>=1632700800 
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
 and try_cast(server_id as int ) between 2 and 356
 group by role_id,server_id,
 DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
 )as tmp
where nums>=380000
) as t1
inner join
( select role_id,base_level,DATE_FORMAT( FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d' ) as daily
    ,row_number() over(partition by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d' )
     order by base_level ) as rank
from log_activity_score
where ac_id='1000300'
 and created_at<1633910400
 and created_at>=1632700800 
)as t2
on t1.role_id=t2.role_id and t1.daily=t2.daily
where rank=1