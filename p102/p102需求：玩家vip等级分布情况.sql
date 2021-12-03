1.需求：玩家vip等级分布情况
2.需求目的：每周vip等级数据情况分析
3.筛选条件：
全服截止到今天vip各等级玩家数量
4.数据格式：
服务器，vip等级，玩家数 @小乔 

select server_id,vip_level,count(distinct role_id) as uv
from 
(
select role_id,server_id,vip_level
from
(
select role_id,server_id,vip_level,row_number() over(partition by role_id order by created_at desc) as rank
from log_vip
where role_id in (select uuid from roles where is_internal=false and server_id!='1')
) as tmp1 where rank=1

) as tmp2 group by server_id,vip_level