@动人 ，一个提数需求，感谢：
服务器 全服
目标玩家 主堡15级以上

玩家的 
①国家
②历史付费
③玩家创角的第30日~100日，每5日的主堡等级，若在该天无登录记录，记为0
select role_id,reg_day,country,paid,cur_level,
		sum( CASE WHEN diff = 30 THEN base_level ELSE 0 END ) day30,
		sum( CASE WHEN diff = 35 THEN base_level ELSE 0 END ) day35,
		sum( CASE WHEN diff = 40 THEN base_level ELSE 0 END ) day40,
		sum( CASE WHEN diff = 45 THEN base_level ELSE 0 END ) day45,
		sum( CASE WHEN diff = 50 THEN base_level ELSE 0 END ) day50,
		sum( CASE WHEN diff = 55 THEN base_level ELSE 0 END ) day55,
		sum( CASE WHEN diff = 60 THEN base_level ELSE 0 END ) day60,
		sum( CASE WHEN diff = 65 THEN base_level ELSE 0 END ) day65,
		sum( CASE WHEN diff = 70 THEN base_level ELSE 0 END ) day70,
		sum( CASE WHEN diff = 75 THEN base_level ELSE 0 END ) day75,
		sum( CASE WHEN diff = 80 THEN base_level ELSE 0 END ) day80,
		sum( CASE WHEN diff = 85 THEN base_level ELSE 0 END ) day85,
		sum( CASE WHEN diff = 90 THEN base_level ELSE 0 END ) day90,
		sum( CASE WHEN diff = 95 THEN base_level ELSE 0 END ) day95,
		sum( CASE WHEN diff = 100 THEN base_level ELSE 0 END ) day100
	

from
(
select *,DATE_DIFF ( 'day', DATE_PARSE ( reg_day, '%Y-%m-%d' ), DATE_PARSE (log_day, '%Y-%m-%d' ) ) AS diff
from(
select role_id,paid,roles.base_level as cur_level,roles.country,
		log_logout.base_level as 	base_level,
		DATE_FORMAT( FROM_UNIXTIME( log_logout.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as log_day,
		DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as reg_day
		,row_number() over(partition by role_id,DATE_FORMAT( FROM_UNIXTIME( log_logout.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d') order by log_logout.base_level desc) as rank

from log_logout
inner join roles on roles.uuid=log_logout.role_id
where roles.base_level>=15 and roles.is_internal=false 
)as t1
where rank=1
) as t2
group by role_id,reg_day,country,paid,cur_level
