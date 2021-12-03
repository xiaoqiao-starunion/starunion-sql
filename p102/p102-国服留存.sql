
国服 需要切换账号，服务器记的utc时间，北京时间需要在+08:00，时间戳转换使用正常北京时间。


select daily ,reg_time,country,
		sum(case when diff =0  then base_level else null end) as base_level_0,
		sum(case when diff=1  then base_level else null end) as base_level_1,
		sum(case when diff=2  then base_level else null end) as base_level_2,
		role_id,day2,day3,day4
from	
(	
select
	daily ,reg_time,country,temp1.role_id,day2,day3,day4,
	DATE_DIFF ( 'day',DATE_PARSE ( daily, '%Y-%m-%d' ), DATE_PARSE (login_day, '%Y-%m-%d' ) ) as diff,
	case when logout_level is not null and login_level is not null and login_level>=logout_level then login_level
	    when logout_level is not null  and login_level is not null and login_level<logout_level then logout_level
		when logout_level is null then login_level --没有生效 检查一下
		
		when login_level is null then logout_level

 		else 0 end  as base_level

from
(
	SELECT
		
		log_login_role.daily,reg_time,role_id,country,
		sum( CASE WHEN diff = 1 THEN 1 ELSE 0 END ) day2,
		sum( CASE WHEN diff = 2 THEN 1 ELSE 0 END ) day3,
		sum( CASE WHEN diff = 3 THEN 1 ELSE 0 END ) day4
		
	FROM
		(
		SELECT
			log_login_role.role_id,roles.country,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ) AS daily,
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d %H:%I' ) AS reg_time,
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ) AS loginDay,
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ), '%Y-%m-%d' )) AS diff
			
		FROM log_login_role
			INNER JOIN ( SELECT roles.uuid, country, roles.created_at FROM roles WHERE roles.is_internal = FALSE ) roles ON roles.uuid = log_login_role.role_id 
	 where roles.created_at>=1629129600 and roles.created_at<1629388800--北京注册时间限制17-19
			   
		GROUP BY
			log_login_role.role_id,roles.country,
			
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ),
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d %H:%I' ),
			DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ),
			DATE_DIFF (
				'day',
				DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ), '%Y-%m-%d' ),
			DATE_PARSE ( DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ), '%Y-%m-%d' ))
			
		) log_login_role 
	GROUP BY
		
		log_login_role.daily,role_id,reg_time,country
	 
	) AS temp1 
	
	left join (

		select role_id,base_level as login_level,
       			DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ) as login_day,
       			row_number() over(partition by role_id,
       				DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' )
       				order by base_level desc) as rank2 --取每天的登录最大等级记录

       from log_login_role 

       where created_at>=1629129600 
		) as temp2
	on temp1.role_id=temp2.role_id 
     	inner join (
       select role_id,base_level as logout_level,
       			DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' ) as logout_day,
       			row_number() over(partition by role_id,
       				DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+08:00', '%Y-%m-%d' )
       				order by created_at desc) as rank1 --取每天的最后一登出记录

       from log_logout

       where created_at>=1629129600 
		)as temp3
		on temp2.role_id=temp3.role_id and temp2.logout_day=temp3.login_day
	where rank1=1 and DATE_DIFF ( 'day', DATE_PARSE ( daily, '%Y-%m-%d' ), DATE_PARSE (login_day, '%Y-%m-%d' ) ) <=2
			and rank2=1
		) as tmp
	-- and country ='TW'
	group by daily ,reg_time,country,role_id,day2,day3,day4
	

	
	