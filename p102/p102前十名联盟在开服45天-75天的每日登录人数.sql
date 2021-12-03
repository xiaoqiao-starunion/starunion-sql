开服75天以上的服务器，前十名联盟在开服45天-75天的每日登录人数
表结构：
联盟创建时间，区服，排名，联盟ID，盟主ID，联盟战力，语种，充值金额，成员地区占比，联盟人数，DAY45~day75 @动人 
with top10_alliances as
(
select all.server_id,open_day,rank,all.created_at,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
from
(
select  server_id,created_at
		,row_number() over(partition by server_id order by alliance_power desc) as rank
		,alliance_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as create_day
		,alliance_roles,alliance_owner_id,alliance_power,alliance_language
from alliances
)as all
inner join 
( select uuid,open_day,DATE_DIFF ( 'day', DATE_PARSE ( open_day, '%Y-%m-%d' ), 
					DATE_PARSE (cur_day, '%Y-%m-%d' ) ) as diff
  from
  (
	select uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as open_day
	 	, DATE_FORMAT( now()  AT TIME ZONE 'UTC', '%Y-%m-%d' ) as cur_day

  	from servers
  )as t
  where DATE_DIFF ( 'day', DATE_PARSE ( open_day, '%Y-%m-%d' ), 
					DATE_PARSE (cur_day, '%Y-%m-%d' ) )>=75
)as ser on all.server_id=ser.uuid
where rank<=10
),


log as (

select server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		, 
		count(distinct CASE WHEN diff = 45 THEN uuid ELSE null END ) day45,
		count(distinct CASE WHEN diff = 46 THEN uuid ELSE null END ) day46,
		count(distinct CASE WHEN diff = 47 THEN uuid ELSE null END ) day47,
		count(distinct CASE WHEN diff = 48 THEN uuid ELSE null END ) day48,
		count(distinct CASE WHEN diff = 49 THEN uuid ELSE null END ) day49,
		count(distinct CASE WHEN diff = 50 THEN uuid ELSE null END ) day50,
		count(distinct CASE WHEN diff = 51 THEN uuid ELSE null END ) day51,
		count(distinct CASE WHEN diff = 52 THEN uuid ELSE null END ) day52,
		count(distinct CASE WHEN diff = 53 THEN uuid ELSE null END ) day53,
		count(distinct CASE WHEN diff = 54 THEN uuid ELSE null END ) day54,
		count(distinct CASE WHEN diff = 55 THEN uuid ELSE null END ) day55,
		count(distinct CASE WHEN diff = 56 THEN uuid ELSE null END ) day56,
		count(distinct CASE WHEN diff = 57 THEN uuid ELSE null END ) day57,
		count(distinct CASE WHEN diff = 58 THEN uuid ELSE null END ) day58,
		count(distinct CASE WHEN diff = 59 THEN uuid ELSE null END ) day59,
		count(distinct CASE WHEN diff = 60 THEN uuid ELSE null END ) day60,
		count(distinct CASE WHEN diff = 61 THEN uuid ELSE null END ) day61,
		count(distinct CASE WHEN diff = 62 THEN uuid ELSE null END ) day62,
		count(distinct CASE WHEN diff = 63 THEN uuid ELSE null END ) day63,
		count(distinct CASE WHEN diff = 64 THEN uuid ELSE null END ) day64,
		count(distinct CASE WHEN diff = 65 THEN uuid ELSE null END ) day65,
		count(distinct CASE WHEN diff = 66 THEN uuid ELSE null END ) day66,
		count(distinct CASE WHEN diff = 67 THEN uuid ELSE null END ) day67,
		count(distinct CASE WHEN diff = 68 THEN uuid ELSE null END ) day68,
		count(distinct CASE WHEN diff = 69 THEN uuid ELSE null END ) day69,
		count(distinct CASE WHEN diff = 70 THEN uuid ELSE null END ) day70,
		count(distinct CASE WHEN diff = 71 THEN uuid ELSE null END ) day71,
		count(distinct CASE WHEN diff = 72 THEN uuid ELSE null END ) day72,
		count(distinct CASE WHEN diff = 73 THEN uuid ELSE null END ) day73,
		count(distinct CASE WHEN diff = 74 THEN uuid ELSE null END ) day74,
		count(distinct CASE WHEN diff = 75 THEN uuid ELSE null END ) day75
		
		
from
(select server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,uuid,country,log_day,DATE_DIFF ( 'day', DATE_PARSE ( open_day, '%Y-%m-%d' ), 
					DATE_PARSE (log_day, '%Y-%m-%d' ) ) as diff
from 
(
select server_id,open_day,rank,create_day,top10_alliances.alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,uuid,country
from top10_alliances
inner join(
	select uuid,alliance_id,country
	from roles
	where server_id!='1' and is_internal=false
) as cur_users on  top10_alliances.alliance_id=cur_users.alliance_id
) as users
left join (
	select role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
	from log_login_role
	group by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as log on users.uuid=log.role_id 
) as tmp group by server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
),		
		

country as (
select server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,
		case when country ='TW' then 'TW'
		     when  country ='BR' then 'BR'
		      when  country ='CN' then 'CN'
		       when  country ='DE' then 'DE'
		        when  country ='FR' then 'FR'
		         when  country ='ID' then 'ID'
		          when  country ='JP' then 'JP'
		           when  country ='KR' then 'KR'
		            when  country ='PH' then 'PH'
		             when  country ='TH' then 'TH'
		             when  country ='US' then 'US'
		             else 'qita' end as country,

			count(distinct uuid) as uv

		
		
from
(select server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,uuid,country
from 
(
select server_id,open_day,rank,create_day,top10_alliances.alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,uuid,country
from top10_alliances
inner join(
	select uuid,alliance_id,country
	from roles
	where server_id!='1' and is_internal=false
) as cur_users on  top10_alliances.alliance_id=cur_users.alliance_id
) as users
) as tmp group by server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,case when country ='TW' then 'TW'
		     when  country ='BR' then 'BR'
		      when  country ='CN' then 'CN'
		       when  country ='DE' then 'DE'
		        when  country ='FR' then 'FR'
		         when  country ='ID' then 'ID'
		          when  country ='JP' then 'JP'
		           when  country ='KR' then 'KR'
		            when  country ='PH' then 'PH'
		             when  country ='TH' then 'TH'
		             when  country ='US' then 'US'
		             else 'qita' end
),


paid as (

select server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,sum(price) as money
from 
(
select server_id,open_day,rank,create_day,created_at,top10_alliances.alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
		,uuid,country
from top10_alliances
inner join(
	select uuid,alliance_id,country
	from roles
	where server_id!='1' and is_internal=false
) as cur_users on  top10_alliances.alliance_id=cur_users.alliance_id
) as users
left join (
		select role_id,created_at as pay_time,price
		from payments
		where is_paid=1 and status=2 and is_test=2
    		and role_id in (select uuid from roles where server_id!='1' and is_internal=false)
	
)as pay on users.uuid=pay.role_id 
where pay_time>created_at
 group by server_id,open_day,rank,create_day,alliance_id,alliance_roles,alliance_owner_id,alliance_power,alliance_language
) 		



		
