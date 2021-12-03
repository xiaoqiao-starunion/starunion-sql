1.需求：玩家活跃留存
2.需求目的：分析241-421服务器的玩家活跃留存情况
3.筛选条件：服务器：241~421服；时间：8.1~10.14
4.数据格式：（表头）
日期，服务器，2~7，14，30，60留

活跃留存：某日活跃的账号中，在日后第X日有登录行为记为留存



	SELECT loginDay,server_id ,
	sum(case when diff=0 then 1 else 0 end )day1, --当天活跃人数，包括新老用户
	sum(case when diff=1 then 1 else 0 end )day2,
	sum(case when diff=2 then 1 else 0 end )day3,
	sum(case when diff=3 then 1 else 0 end )day4,
	sum(case when diff=4 then 1 else 0 end )day5,
	sum(case when diff=5 then 1 else 0 end )day6,
	sum(case when diff=6 then 1 else 0 end )day7,
	sum(case when diff=13 then 1 else 0 end )day14,
	sum(case when diff=29 then 1 else 0 end )day30,
	sum(case when diff=59 then 1 else 0 end )day60 
	FROM (SELECT log1.role_id,log1.server_id,
		log1.loginDay,
		DATE_DIFF( 'day',DATE_PARSE(log1.loginDay,'%Y-%m-%d'),
			DATE_PARSE(log2.loginDay,'%Y-%m-%d')) as diff
		from
		( select distinct role_id,server_id ,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00' ,'%Y-%m-%d') as loginDay
			FROM log_login_role log1
			where created_at>=1627776000 and created_at<=1634255999
				and ((year=2021 and month=7 and day>=30 and day<=31) or (year=2021 and month=8 ) or (year=2021 and month=9 ) 
			or (year=2021 and month=10 and day>=1 and day<=16)) 
				and try_cast(server_id as int) between 241 and 421
				and role_id in (SELECT uuid from roles where is_internal=false and server_id!='1')
		) as log1
		left join
		(	select distinct role_id,server_id ,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00' ,'%Y-%m-%d') as loginDay
			FROM log_login_role log1
			where created_at>=1627776000 
				and month>=7
				and try_cast(server_id as int) between 241 and 421
		) as log2 on log1.role_id=log2.role_id
		
		
		)as tmp
		where diff>=0
		 GROUP BY server_id,loginDay
order by loginDay,server_id