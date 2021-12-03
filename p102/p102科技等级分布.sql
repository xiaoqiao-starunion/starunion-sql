目标用户：9.22登陆过的付费用户（22日之前付费过就算）
服务器：S140-S380
查询日期：9.22  23:59
条件：科技进度分类平均百分比(如急速生产X%)，按照游戏语言（英文，中文（包括繁体），台湾，日本，韩国）分语言输出。再输出一份分服分语言的，按照50个服一组。 @小乔 

select count(distinct role_id) as uv , language,tech_id,tech_level
from
(
select a.role_id,language,server_id,tech_id,tech_level
from
( 	select role_id,language,server_id
		from
	(select distinct role_id
		from log_login_role
		where created_at<1632355200 and created_at>=1632268800 --22号
	)as t1
	inner join
	(
		select uuid,language,server_id
		from roles
		where  paid>0 and first_paid_time<1632355200 --首次付费时间在22号之前
		and try_cast(server_id as int) between 140 and 380
	) as t2 on t1.role_id=t2.uuid
)as a

inner join (
select role_id,tech_id,tech_level,
row_number() over(partition by role_id,tech_id order by created_at desc) as rank
from log_tech
where year=2021
and action in ('6401','6402')
) as b 
on a.role_id=b.role_id
where language in (1,2,3,7,8) and rank=1
) as tmp
group by language,tech_id,tech_level



--分服

select count(distinct role_id) as uv , language,tech_id,tech_level,server_range
from
(
select a.role_id,language,server_id,tech_id,tech_level,
		case when try_cast(server_id as int) between 140 and 189 then '140-189服'
		when try_cast(server_id as int) between 190 and 239 then '190-239服'
		when try_cast(server_id as int) between 240 and 289 then '240-289服'
		when try_cast(server_id as int) between 290 and 339 then '290-339服'
		when try_cast(server_id as int) between 340 and 380 then '340-380服'
	
		else '其他'
		end as server_range



from
( 	select role_id,language,server_id
		from
	(select distinct role_id
		from log_login_role
		where created_at<1632355200 and created_at>=1632268800 --22号
	)as t1
	inner join
	(
		select uuid,language,server_id
		from roles
		where  paid>0 and first_paid_time<1632355200 --首次付费时间在22号之前
		and try_cast(server_id as int) between 140 and 380
	) as t2 on t1.role_id=t2.uuid
)as a

inner join (
select role_id,tech_id,tech_level,
row_number() over(partition by role_id,tech_id order by created_at desc) as rank
from log_tech
where year=2021
and action in ('6401','6402')
) as b 
on a.role_id=b.role_id
where language in (1,2,3,7,8) and rank=1
) as tmp
group by language,tech_id,tech_level,server_range