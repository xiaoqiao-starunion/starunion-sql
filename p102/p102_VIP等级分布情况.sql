需求：VIP等级分布情况
需求目的：看目前玩家VIP等级情况
筛选条件：UTC 11.13 23:59时的数据，11.12到至今有登录行为，VIP等级，对应VIP等级的玩家数量，玩家语言
数据格式：VIP等级，玩家数量，玩家语言

select vip_level,language,count(distinct role_id) as uv
from
(
select t0.role_id,vip_level,language
from
(
select distinct role_id
from log_login_role
where created_at>=1636675200
) as t0
inner join
(
select role_id,vip_level,language
from(
select role_id,vip_level,row_number() over(partition by role_id order by created_at desc) as rank
from log_vip
where created_at<1636848000
) as t1 
inner join 
( select uuid,language
from roles
where is_internal=false and server_id != '1'
) as t2 

on t1.role_id=t2.uuid
  where rank=1
) as t3
on t0.role_id=t3.role_id
) as tmp
group by vip_level,language