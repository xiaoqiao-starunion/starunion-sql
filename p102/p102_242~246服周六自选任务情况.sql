242~246服周六自选任务情况
时间为8月28日，9月4日
表结构：服务器，日期，DAU,建筑任务人数，采集任务人数，进化任务人数，特化蚁任务人数，兵蚁任务人数 @小乔 
8001	购买获得钻石	所有任务
8003	采集	采集任务
8004	建筑战力	建筑任务
8007	科技战力	进化任务
8008	英雄招募	特化蚁任务
8009	消耗变异孢子	特化蚁任务
8010	兑换变异孢子	特化蚁任务
8011	英雄获得经验	特化蚁任务
8012	解锁英雄技能	特化蚁任务
8013	打野怪	特化蚁任务
8014	训练士兵	兵蚁任务
8015	强化士兵	兵蚁任务
8016	晋级士兵	兵蚁任务
8035

select log_day,t2.server_id,dau,task_type,uv,pv
from
(
select count(distinct role_id) as uv,count(role_id) as pv
		,act_day,server_id,task_type
from
(
select role_id,server_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
		,case when action in ('8003') then '采集任务'
				when action in ('8004') then '建筑任务'
				when action in ('8007' ,'8035') then '进化任务'
				when action in ('8008','8009','8010','8011','8012') then '特化蚁任务'
				when action in ('8014','8015','8016') then '兵蚁任务'
				else '其他任务' end as task_type
from log_activity_score
where DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) in ('2021-08-28','2021-09-04')
 and action in ('8001','8003','8004','8007','8008','8009','8010','8011','8012','8013','8014','8015','8016','8035')
 and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
 and try_cast(server_id as integer) between 242 and 246
  and nums>0
) as tmp group by act_day,task_type,server_id
) as t1

inner join(
select count(distinct role_id) as dau
		,server_id
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
from log_login_role
where role_id in (select uuid from roles where is_internal=false and server_id!='1')
	 and DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) in ('2021-08-28','2021-09-04')
	 and try_cast(server_id as integer) between 242 and 246
group by server_id
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as t2 on t1.server_id=t2.server_id and log_day=act_day
