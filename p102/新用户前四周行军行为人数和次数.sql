前四周行军行为
select reg_day,category,

		COALESCE(count(distinct uuid),0) as uv,
		sum(COALESCE(nums,0)) as pv,
		case when diff>=0 and diff<7 then '第一周'
				when diff>=7 and diff<14 then '第二周'
				when diff>=14 and diff<21 then '第三周'
				when diff>=21 and diff<28 then '第四周'
				else 'qita' end as date_range
from
(
select uuid,reg_day,mis_day,category,nums,
		DATE_DIFF ( 'day', DATE_PARSE ( reg_day, '%Y-%m-%d' ), DATE_PARSE (mis_day, '%Y-%m-%d' ) ) AS diff
from
(select uuid
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as reg_day


from roles
where is_internal=false 
 		-- and server_id!='1'

)roles 
inner join
( 
	 SELECT
			role_id,
			DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) mis_day,
			count( role_id ) AS nums,
			category 
		FROM
			log_missions 
		WHERE
			is_return = 0 
			AND YEAR = 2021 
			AND category IN ( 1,2, 4, 7 ) 
		GROUP BY
			role_id,
			DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ),
			category 
)as log_mis
on  log_mis.role_id=roles.uuid
) as t 
group by reg_day,category,case when diff>=0 and diff<7 then '第一周'
				when diff>=7 and diff<14 then '第二周'
				when diff>=14 and diff<21 then '第三周'
				when diff>=21 and diff<28 then '第四周'
				else 'qita' 









select reg_day,category,

		COALESCE(count(distinct role_id),0) as uv,
		sum(COALESCE(nums,0)) as pv,
		case when diff>=0 and diff<7 then '第一周'
				when diff>=7 and diff<14 then '第二周'
				when diff>=14 and diff<21 then '第三周'
				when diff>=21 and diff<28 then '第四周'
				else 'qita' end as date_range
from
(
	select a1.*,a2.device_id
	from(
select uuid,reg_day,mis_day,category,nums,
		DATE_DIFF ( 'day', DATE_PARSE ( reg_day, '%Y-%m-%d' ), DATE_PARSE (mis_day, '%Y-%m-%d' ) ) AS diff
from
(select uuid
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d')  as reg_day


from roles
where is_internal=false 
 		-- and server_id!='1'

)roles 
inner join
( 
	 SELECT
			role_id,
			DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) mis_day,
			count( role_id ) AS nums,
			category 
		FROM
			log_missions 
		WHERE
			is_return = 0 
			AND YEAR = 2021 
			AND category IN ( 1,2, 4, 7 ) 
		GROUP BY
			role_id,
			DATE_FORMAT( FROM_UNIXTIME( log_missions.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ),
			category 
)as log_mis
on  log_mis.role_id=roles.uuid
) as a1 left join 
	( select role_id, device_id -- 大号
		from
		(
		select role_id ,device_id,base_level,row_number() over(partition by device_id order by base_level desc) as rank2
		from
		(
		select role_id ,device_id,base_level,row_number() over(partition by role_id order by base_level desc) as rank1
		from log_logout
		group by role_id,device_id,base_level
		) temp1 where rank1=1
		) temp2 
        where rank2=1
		) as a2
	on a1.uuid=a2.role_id

) as t 
group by reg_day,category,case when diff>=0 and diff<7 then '第一周'
				when diff>=7 and diff<14 then '第二周'
				when diff>=14 and diff<21 then '第三周'
				when diff>=21 and diff<28 then '第四周'
				else 'qita' end				