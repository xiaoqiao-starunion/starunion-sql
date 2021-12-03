p102生物残骸余量


select role_id,server_id,name,paid,balance,action_day --最后一次余量变动时间
from
(	select role_id, balance,action_day
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id=204066
	and month>=9
	) as t where rank=1 and balance>=10000
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id
