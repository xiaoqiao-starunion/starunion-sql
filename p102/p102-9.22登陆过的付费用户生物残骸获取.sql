目标用户：9.22登陆过的付费用户（22日之前付费过就算）20210923
服务器：S140-S380
查询日期：9.16-9.22
条件：分日人均生物残骸获得量，消耗量，获取消耗总量，按照游戏语言（英文，中文（包括繁体），台湾，日本，韩国）分语言输出。


select language,act_day,count(distinct role_id) as uv,sum(nums) as sum

from

(
select a.role_id,nums,language,act_day
from
( 	select role_id,language
		from
	(select distinct role_id
		from log_login_role
		where created_at<1632355200 and created_at>=1632268800 --22号
	)as t1
	inner join
	(
		select uuid,language
		from roles
		where  paid>0 and first_paid_time<1632355200 --首次付费时间在22号之前
	) as t2 on t1.role_id=t2.uuid
)as a
inner join
(
select role_id,nums,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
from log_item
where item_id=204066 and nums>0
and created_at>1631750400 and created_at<1632355200
and try_cast(server_id as int) between 140 and 380

) as b on a.role_id=b.role_id
) as tmp
group by language,act_day


--消耗
select language,act_day,count(distinct role_id) as uv,sum(nums) as sum

from

(
select a.role_id,nums,language,act_day
from
( 	select role_id,language
		from
	(select distinct role_id
		from log_login_role
		where created_at<1632355200 and created_at>=1632268800 --22号
	)as t1
	inner join
	(
		select uuid,language
		from roles
		where  paid>0 and first_paid_time<1632355200 --首次付费时间在22号之前
	) as t2 on t1.role_id=t2.uuid
)as a
inner join
(
select role_id,nums,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
from log_item
where item_id=204066 and nums<0
and created_at>1631750400 and created_at<1632355200
and try_cast(server_id as int) between 140 and 380

) as b on a.role_id=b.role_id
) as tmp
group by language,act_day