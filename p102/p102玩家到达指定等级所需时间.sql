1.需求：玩家到达指定等级所需时间

2.需求目的：分析游戏语言为俄语的玩家到达指定等级所需时间情况

3.筛选条件
时间：3.8~10.17
条件：
游戏语言为俄语 且 无充值记录玩家

达到LV8 平均所需时长（时间单位为分钟）
达到LV10 平均所需时长（时间单位为分钟）
达到LV13 平均所需时长（时间单位为分钟）
达到LV15 平均所需时长（时间单位为分钟）
达到LV17 平均所需时长（时间单位为分钟）

4.数据结构
达到等级，所需时间



select '达到LV8' ,sum(r_times)/ count(distinct uuid) as per_times
from
(
select uuid,paid,language,reg_t,login_time,(login_time-reg_t)/60 as r_times
from
(
select uuid,paid,language,created_at  as reg_t
from roles 
where language=10 and paid=0
and is_internal=false and server_id!='1'
) as t1
left join
( select role_id,base_level,created_at as login_time,row_number() over(partition by role_id order by created_at) as rank -- 第一次8级的登录时间
	from log_logout
	where base_level>=8

) as t2
on t1.uuid=t2.role_id 
where rank=1
) as a

union all

select '达到LV10' ,sum(r_times)/ count(distinct uuid) as per_times
from
(
select uuid,paid,language,reg_t,login_time,(login_time-reg_t)/60 as r_times
from
(
select uuid,paid,language,created_at  as reg_t
from roles 
where language=10 and paid=0
and is_internal=false and server_id!='1'
) as t1
left join
( select role_id,base_level,created_at as login_time,row_number() over(partition by role_id order by created_at) as rank -- 第一次8级的登录时间
	from log_logout
	where base_level>=10

) as t2
on t1.uuid=t2.role_id 
where rank=1
) as a

union all

select '达到LV13' ,sum(r_times)/ count(distinct uuid) as per_times
from
(
select uuid,paid,language,reg_t,login_time,(login_time-reg_t)/60 as r_times
from
(
select uuid,paid,language,created_at  as reg_t
from roles 
where language=10 and paid=0
and is_internal=false and server_id!='1'
) as t1
left join
( select role_id,base_level,created_at as login_time,row_number() over(partition by role_id order by created_at) as rank -- 第一次8级的登录时间
	from log_logout
	where base_level>=13

) as t2
on t1.uuid=t2.role_id 
where rank=1
) as a

union all

select '达到LV15' ,sum(r_times)/ count(distinct uuid) as per_times
from
(
select uuid,paid,language,reg_t,login_time,(login_time-reg_t)/60 as r_times
from
(
select uuid,paid,language,created_at  as reg_t
from roles 
where language=10 and paid=0
and is_internal=false and server_id!='1'
) as t1
left join
( select role_id,base_level,created_at as login_time,row_number() over(partition by role_id order by created_at) as rank -- 第一次8级的登录时间
	from log_logout
	where base_level>=15

) as t2
on t1.uuid=t2.role_id 
where rank=1
) as a

union all

select '达到LV17' ,sum(r_times)/ count(distinct uuid) as per_times
from
(
select uuid,paid,language,reg_t,login_time,(login_time-reg_t)/60 as r_times
from
(
select uuid,paid,language,created_at  as reg_t
from roles 
where language=10 and paid=0
and is_internal=false and server_id!='1'
) as t1
left join
( select role_id,base_level,created_at as login_time,row_number() over(partition by role_id order by created_at) as rank -- 第一次8级的登录时间
	from log_logout
	where base_level>=17

) as t2
on t1.uuid=t2.role_id 
where rank=1
) as a

