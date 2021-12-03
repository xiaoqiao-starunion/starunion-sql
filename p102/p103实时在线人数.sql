@动人 大佬一个提数需求
1. 需求：
S14~S16 创角角色，在10.21~10.24时间段内 以分钟为基准，分日的在线峰值。
2. 需求目的：
 37海外测试数据需求
3. 筛选条件：
服务器: S14~S16
目标玩家：UTC 10.21 00:00:00~UTC 10.24 23:59:59 创角的玩家角色
4. 数据格式如下：
select max(max_users) as max_users
from(
	
		SELECT sum(max_users) as max_users,create_time
				
		FROM
		(
		SELECT
			max( now_users ) AS max_users,
			min( now_users ) min_users,
			avg( now_users ) avg_users,
			sum(
			IF
			( MOD ( floor( created_at / 60 )* 60, 60 )= 0, now_users, 0 )) now_users,
			DATE_FORMAT( from_unixtime( floor( created_at / 60 )* 60 ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%i:%s' )
						AS create_time,
					
			server_id 
		FROM
			log_onlines  --已经聚合 没有玩家ID
		where server_id in ('S14','S15','S16')
		GROUP BY
			DATE_FORMAT( from_unixtime( floor( created_at / 60 )* 60 ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%i:%s' ),
			server_id
			
		ORDER BY
			create_time DESC,
			server_id 
		) tab_temp
		group by create_time
	
) as t1 



--平台实时在线的代码
	select * from 
	(select max(now_users) as max_users,
		min(now_users)  min_users,
		avg(now_users)  avg_users,
		sum(if(mod(floor(created_at/60)*60,60)=0,now_users,0))  now_users,
		DATE_FORMAT(from_unixtime(floor(created_at/60)*60) AT TIME ZONE 'UTC' ,'%Y-%m-%d %H:%i:%s') as create_time,
		server_id 
		from log_onlines  
		where  created_at>= 1634774400 AND created_at <= 1635119999 and 
		(year=2021 and month=10 and day>=20 and day<=25) 
		and server_id  IN  ('S16','S15','S14') 
		group by DATE_FORMAT(from_unixtime(floor(created_at/60)*60) AT TIME ZONE 'UTC' ,'%Y-%m-%d %H:%i:%s'),
		server_id order by create_time desc,server_id)tab,(select avg(now_users) 
		as total_avg_users from log_onlines  
		where  created_at>= 1634774400 AND created_at <= 1635119999 and (year=2021 and month=10 and day>=20 and day<=25) 
		and server_id  IN  ('S16','S15','S14'))tab_onlines