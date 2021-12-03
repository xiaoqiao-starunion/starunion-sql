钻石余量=道具+资源
select *
from 
(select role_id,server_id,name,paid,sum(balance)
from
(

select role_id,server_id,name,paid,
	sum(balance) as balance
from
(	select role_id,item_id,
		case when item_id=200073 then balance*5
				when item_id=200074 then balance*10
				when item_id=200075 then balance*20
				when item_id=200076 then balance*50
				when item_id=200077 then balance*100
				when item_id=200078 then balance*200
				when item_id=200079 then balance*1000
				when item_id=200080 then balance*5000
				end as balance

	from
	(
	SELECT role_id,item_id,balance,
			
			row_number() over(partition by role_id,item_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where item_id between 200073 and 200080
	-- and month>=8
	) as t where rank=1  
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id

group by role_id,server_id,name,paid



union 

	select role_id,server_id,name,paid,balance
	
from
(	select  role_id,balance

	from
	(
	SELECT role_id,resource_id,balance,
			
			row_number() over(partition by role_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_resource 
	where resource_id =1

	) as t where rank=1  
) as users
inner join 
( select uuid,server_id,paid,name
	from roles
	where is_internal=false and server_id!='1'
) as roles
on roles.uuid=users.role_id

) as tmp
group by role_id,server_id,name,paid
) as tmp2
where balance >=100000

