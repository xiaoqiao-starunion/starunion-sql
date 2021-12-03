需求内容：
创号24小时内能到5级的角色的UTC留存数据（1份美国maia，1份美国全体）

需求目的：
对比39服高质玩家的留存变化

筛选条件：
①角色创建时间UTC 2021/2/3-2021/10/11
②创号24小时内完成基地升级至5级
③角色国家为美国（导出1份美国全体）
④角色渠道为MAIA（导出1份美国maia）

数据格式：
每天一行，包含：日期、创建数、2留数、3留数……90留数


select reg_date,diff,count(distinct uuid)as uv
from
(
select reg_date,uuid,DATE_DIFF (
				'day',
				DATE_PARSE ( reg_date, '%Y-%m-%d' ),
			DATE_PARSE (log_date, '%Y-%m-%d' )) AS diff
from 
(
select distinct uuid,reg_date
from(
select uuid,created_at,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS reg_date
from roles
where created_at>=1612310400 and created_at<1633996800
		and country='US' and is_internal=false
)as reg 
inner join (
	select role_id,created_at,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS log_date
	from log_logout
)as log on uuid=role_id
where (log.created_at-reg.created_at)<=86400 and base_level>=5  
)as t1
left join (
select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS log_date
from log_login_role
)as t2
on t1.uuid=t2.role_id
) as tmp 
where diff<=90
group by reg_date,diff



select reg_date,diff,count(distinct uuid)as uv
from
(
select reg_date,uuid,DATE_DIFF (
				'day',
				DATE_PARSE ( reg_date, '%Y-%m-%d' ),
			DATE_PARSE (log_date, '%Y-%m-%d' )) AS diff
from 
(
select distinct uuid,reg_date
from(
select uuid,created_at,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS reg_date
from roles
where created_at>=1612310400 and created_at<1633996800
		and country='US' and is_internal=false and position( 'MAIA' IN campaign ) > 0
)as reg 
inner join (
	select role_id,created_at,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS log_date
	from log_logout
)as log on uuid=role_id
where (log.created_at-reg.created_at)<=86400 and base_level>=5  
)as t1
left join (
select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC','%Y-%m-%d') AS log_date
from log_login_role
)as t2
on t1.uuid=t2.role_id
) as tmp 
where diff<=90
group by reg_date,diff

