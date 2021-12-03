需求：溪蟹作为升星材料使用情况
需求目的：查看是哪些玩家将溪蟹当作了升星材料去培养其他野怪
筛选条件：日期：10.29-11.14
升星使用材料为：72014000：溪蟹
数据结构



72014000：溪蟹存量



select server_id,t1.role_id,name,account_id,level,id,amount,paid
from
(
select server_id,role_id,account_id,name,id,a.level,amount
from
(
	select role_id,t1.id,t1.level,t1.amount
	from 
	(select role_id,params,row_number() over(partition by role_id order by created_at desc) as rank
	from log_role_data
	where type=2 and created_at<=1636934400
	) as t0
	cross join unnest(cast(cast(json_parse(params) as array(json)) as array(row(Id INTEGER,level INTEGER,amount INTEGER)))) 
	as temp(t1)
	where rank=1
	) as a inner join roles on roles.uuid=a.role_id
where id=72014000
 and is_internal=false and server_id!='1'
) as t1
left join
( select role_id,sum(price) as paid
	from payments
	where is_paid=1 and is_test=2 and status=2
	and created_at<=1636934400
	group by role_id
)as t2
on t1.role_id=t2.role_id