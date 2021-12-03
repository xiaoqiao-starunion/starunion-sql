需求汇总
1.区域迁徙
需求：每日任务进度分布
目的：了解区域迁徙活动前后不同付费分层玩家数据每日任务进度分布
筛选：
S2-S153，11.09-11.15
玩家类型分：未充值，小R，中R，大R
数据需求：
日期 玩家类型,DAU,[0]人数,(0,30）人数,[30,70)人数,[70,120)人数,[120,180)人数,[180,260)人数,[260,340)人数,[340,420)人数,[420,500)人数,500+人数





select a.type,a.log_day,dau,score_range,uv
from
(
select type,log_day,case when ac_nums is null then '0'
						when ac_nums>0 and ac_nums<30 then '0-30'
						when ac_nums>=30 and ac_nums<70 then '30-70'
						when ac_nums>=70 and ac_nums<120 then '70-120'
						when ac_nums>=120 and ac_nums<180 then '120-180'
						when ac_nums>=180 and ac_nums<260 then '180-260'
						when ac_nums>=260 and ac_nums<340 then '260-340'
						when ac_nums>=340 and ac_nums<420 then '340-420'
						when ac_nums>=420 and ac_nums<500 then '420-500'
						when ac_nums>=500 then '500+'
						end as score_range,
		count(distinct uuid) as uv

from
(select uuid,type,log_day,ac_nums
from
(
select uuid,case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as type
from roles
where is_internal=false and server_id!='1'
and try_cast(server_id as int) between 2 and 153
) as t1
inner join(
	select distinct role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day
	from log_login_role
	where month=11 and created_at>=1637625600 and created_at<1638316800
)as t2
on t1.uuid=t2.role_id
left join
(
	select role_id,sum(nums) as ac_nums,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as ac_day
	from log_activity_score
	where ac_id='1000100' --每日任务
	group by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
) as t3 on t1.uuid=t3.role_id and t2.log_day=t3.ac_day
) as tmp
group by type,log_day,case when ac_nums is null then '0'
						when ac_nums>0 and ac_nums<30 then '0-30'
						when ac_nums>=30 and ac_nums<70 then '30-70'
						when ac_nums>=70 and ac_nums<120 then '70-120'
						when ac_nums>=120 and ac_nums<180 then '120-180'
						when ac_nums>=180 and ac_nums<260 then '180-260'
						when ac_nums>=260 and ac_nums<340 then '260-340'
						when ac_nums>=340 and ac_nums<420 then '340-420'
						when ac_nums>=420 and ac_nums<500 then '420-500'
						when ac_nums>=500 then '500+' end
) as a 
inner join
(
	select count(distinct uuid) as dau,type,log_day
from
(
select uuid,case when paid =0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as type
from roles
where is_internal=false and server_id!='1'
and try_cast(server_id as int) between 2 and 153
) as t1
inner join(
	select distinct role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day
	from log_login_role
	where month=11 and created_at>=1637625600 and created_at<1638316800

)as t2
on t1.uuid=t2.role_id
group by type,log_day
) as b on a.type=b.type and a.log_day=b.log_day

