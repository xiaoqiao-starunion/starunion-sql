服务器：351 354 357 
注册时间：9月4~9月8日 

服务器：373 374 375
注册时间：9月14~9月18日 

输出：注册日期，注册人数，迁服次数为0的玩家，连续登录人数，统计1-7天



with users_base as
(

select uuid,server_id,reg_day
from
(
		select  uuid,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) as reg_day
		from roles
		where created_at >= 1631577600 and created_at<1632009600
		) as a
inner join
( 
		select uuid as role_id,if(last_server_id is not null,last_server_id,server_id) as server_id,country,created_at,account_id
		from
		(
		select uuid,server_id,country,created_at,account_id -- 每个用户的最新ID
		from roles
		where is_internal = false and server_id!='1'
		) as t1
		left join
		(
		select  role_id,last_server_id,server_id as new_server,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%I:%S' ) transfer_date
		    from log_fly
		    where fly_category=204147 and year=2021 and month=9
		    and created_at >=1630886400 and  created_at <1631318400

		) as t2 on t1.uuid=t2.role_id

)as b on a.uuid=b.role_id
where try_cast(server_id as int) in (373,374,375)

)


select reg_day,maxday,count(distinct uuid) as uv
from
(select uuid,server_id,reg_day,maxday
from users_base
left join (
	select role_id,max(continuous_days) as maxday
	from
	(select role_id,
	  date_add('day',-rank1,cast(login_day as date)) as login_group,
	 count(1) as continuous_days
	from
	(
		select role_id,row_number() OVER(PARTITION BY role_id order by login_day) as rank1,login_day
		from
		(select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) login_day
		from log_login_role
		where year=2021 and month=9
		group by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )
		)
	) as tmp1 
     group by role_id,date_add('day',-rank1,cast(login_day as date))
	) as tmp2 group by role_id
)as login on login.role_id=users_base.uuid

) as tmp group by reg_day,maxday
