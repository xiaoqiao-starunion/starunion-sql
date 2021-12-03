P102 道具 野怪卵：204116 玩家存量 
20210909
数据格式：

野怪卵存量 0~4499 玩家人数
野怪卵存量 4500~8999 玩家人数
野怪卵存量 9000~17999 玩家人数
野怪卵存量 18000~35999 玩家人数
野怪卵存量 36000~71999 玩家人数
野怪卵存量 72000~144000 玩家人数

@动人  




select count(distinct role_id) as uv,balance_range
	
from
(	select role_id,item_id, balance,
		case when balance between 0 and 100 then '0~100'
		    when balance between 101 and 300 then '101~300'
		    when balance between 301 and 500  then '301~500'
		    when balance between 501 and 1000  then '501~1000'
		    when balance between 1001 and 2000  then '1001~2000'
		    when balance between 2001 and 3000  then '2001~3000'
		    when balance between 3001 and 4000  then '3001~4000'
		    when balance between 4001 and 5000  then '4001~5000'
		    
		    else 'qita' end as balance_range
		  

	from
	(
	SELECT role_id,item_id,balance,
			row_number() over(partition by role_id,item_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item  
	where item_id=204116 and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
	) as t where rank=1  
) as users
group by balance_range





