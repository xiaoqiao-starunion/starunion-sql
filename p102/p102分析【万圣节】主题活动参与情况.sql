需求：近期主题活动参与情况
需求目的：分析【万圣节】主题活动参与情况
筛选条件：活动时间：【10.19-10.25】、【10.26-11.1】、【11.2-11.8】，活动时间每7天为一个周期，总共3轮，每一轮参与的服务器都不一样（分别是【2-405】、【2-416】、【2-424】），按设备ID取
数据结构1（参与情况）：
活动时间，服务器，满足活动条件的人数（>=6级），参与人数，完成人数（活动奖励满20级），购买通行证人数（29015: 通行证）
数据结构2（各等级完成情况）：
活动时间，服务器，活动等级（20个等级），完成人数
数据结构3（各任务完成情况）：
活动时间，服务器，任务ID（80个任务），完成人数
数据结构4（完成任务数分布）：
活动日期，服务器ID，完成任务个数，人数



with major_users as --每一轮活动，结束前最后一次登出等级>=6
(
    select round,uuid,server_id,reg_day,base_level,last_login_day,paid
    from
(
select uuid,paid,logout.server_id,round,
        DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day 
        ,logout.base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles 
inner join 
(select role_id,server_id,base_level,round,row_number() over(partition by role_id,round order by created_at desc) as rank
from
( select  role_id,server_id,base_level,created_at,
    case when created_at >= 1634601600 and  created_at< 1635206400 then '1'
    when created_at >= 1635206400 and   created_at<1635811200 then '2'
    when created_at >= 1635811200 and   created_at<1636416000 then '3'
    end as round 
from log_logout
where  created_at>=1634601600 and created_at<1636416000
)as tmp
)as logout on logout.role_id=roles.uuid
where is_internal=false and roles.server_id!='1'
       and rank=1 --活动结束前最后一次登出等级   

 ) as a
inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,
            row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
        
    ) b on a.uuid=b.role_id
    where base_level>=6

),
 act as 
(

select round,server_id,count(distinct role_id) as act_uv--参与人数
from
(
select  role_id,log_game_step.server_id,
      case when created_at >= 1634601600 and  created_at< 1635206400  and step_type='ca_five_days_task' and extend_1='1000112'then '1'
           when created_at >= 1635206400 and   created_at<1635811200  and step_type='ca_five_days_task' and extend_1='1000109' then '2'
           when created_at >= 1635811200 and   created_at<1636416000  and step_type='ca_five_days_task' and extend_1='1000127' then '3'
    end as round 
from log_game_step
where  created_at>=1634601600 and created_at<1636416000

    
            
)as tmp 
where role_id in (select distinct uuid from major_users )
and  round is not null
group by round ,server_id

),

finish as (
select  round,ac_id,server_id,count(distinct role_id) as finish_uv --任务完成
from
(select distinct role_id,ac_id,award_node,server_id,
     case when created_at >= 1634601600  and   created_at< 1635206400  and ac_id='1000112' then '1'
           when created_at >= 1635206400 and   created_at<1635811200  and ac_id='1000109' then '2'
           when created_at >= 1635811200 and   created_at<1636416000  and ac_id='1000127' then '3'
    end as round --活动期间，完成任务
from
(select role_id,action,ac_id,award_node,log_activity_award.server_id,
         created_at 
from log_activity_award 
where  created_at>=1634601600 and created_at<1636416000
and action in ('5522')
and ac_id  in ('1000109','1000112','1000127')
and month>=10
) as tmp1  
where role_id in (select distinct uuid from major_users )
and award_node=20
) as tmp2 where round is not null
group by round ,server_id,ac_id

) ,

pay as (

select round,server_id,count(distinct role_id) as pay_uv
FROM
(
select distinct role_id,conf_id,server_id
, case when created_at >= 1634601600  and   created_at< 1635206400  then '1'
           when created_at >= 1635206400 and   created_at<1635811200   then '2'
           when created_at >= 1635811200 and   created_at<1636416000   then '3'
    end as round 
from
(select role_id,conf_id,payments.server_id,created_at
from payments 
where is_paid=1 and is_test=2 and status=2 and conf_id_group='29015'
and  created_at>=1634601600 and created_at<1636416000

) as tmp1 where role_id in (select distinct uuid from major_users )

) as tmp2

group by round ,server_id
)


select t1.round,t1.server_id,uv,act_uv,finish_uv,pay_uv
from
(
select round,server_id,count(distinct uuid) as uv
from major_users 
group by round,server_id
)as t1
inner join act on act.round=t1.round and act.server_id=t1.server_id
left join finish on finish.round=t1.round and finish.server_id=t1.server_id
left join pay on pay.round=t1.round and pay.server_id=t1.server_id





-- 各等级完成情况）

select  round,ac_id,server_id,award_node,count(distinct role_id) as finish_uv --任务完成
from
(select distinct role_id,ac_id,award_node,server_id,
    case when created_at >= 1634601600  and   created_at< 1635206400  and ac_id='1000112' then '1'
           when created_at >= 1635206400 and   created_at<1635811200  and ac_id='1000109' then '2'
           when created_at >= 1635811200 and   created_at<1636416000  and ac_id='1000127' then '3'
    end as round --活动期间，完成任务
from
(select role_id,action,ac_id,award_node,log_activity_award.server_id,
         created_at 
from log_activity_award 
where  created_at>=1634601600 and created_at<1636416000
and action in ('5522')
and ac_id in  ('1000109','1000112','1000127')
and month>=9
) as tmp1  
where role_id in (select distinct uuid from major_users )

) as tmp2 where round is not null
group by round ,server_id,ac_id,award_node



各任务完成情况：mam
活动时间，服务器，任务ID（80个任务），完成人数

select round,server_id,step_id,count(distinct role_id) as act_uv--完成人数
from
(
select role_id,log_game_step.server_id,step_id,extend_1,
     case when created_at >= 1634601600 and  created_at< 1635206400  and step_type='ca_five_days_task' and extend_1='1000112'then '1'
           when created_at >= 1635206400 and   created_at<1635811200  and step_type='ca_five_days_task' and extend_1='1000109' then '2'
           when created_at >= 1635811200 and   created_at<1636416000  and step_type='ca_five_days_task' and extend_1='1000127' then '3'
    end as round 
from log_game_step
    where  created_at>=1634601600 and created_at<1636416000

    and role_id in (select distinct uuid from major_users )
  
) as tmp where round is not null
group by round,server_id,step_id








活动时间，服务器，完成X个任务（1-80个），玩家数，需要去重


select round ,extend_1,server_id,task_nums,count(distinct role_id) as act_uv--完成人数
from
(
select role_id,extend_1,round,server_id,count( distinct step_id) as task_nums --完成任务个数
from    
(        
select role_id,log_game_step.server_id,step_id,extend_1,
   case when created_at >= 1634601600 and  created_at< 1635206400  and step_type='ca_five_days_task' and extend_1='1000112'then '1'
           when created_at >= 1635206400 and   created_at<1635811200  and step_type='ca_five_days_task' and extend_1='1000109' then '2'
           when created_at >= 1635811200 and   created_at<1636416000  and step_type='ca_five_days_task' and extend_1='1000127' then '3'
    end as round 
from log_game_step
    where  created_at>=1634601600 and created_at<1636416000
   
    and role_id in (select distinct uuid from major_users )
 
) as tmp
             
    group by role_id,round,server_id,extend_1
       
)as tmp where round is not null
group by round ,server_id,task_nums,extend_1


