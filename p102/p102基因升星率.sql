基因升星率\获得数量
select t.daily,a.gene_type,a.gene_star,t.country,t.server_id,sum(have_nums+coalesce(nums,0)) as have_nums,sum(coalesce(nums,0)) as "升星量",
			sum(coalesce(nums,0)*1.0)/sum((have_nums+coalesce(nums,0))*1.0)as "升星率",sum(coalesce(get_gene_nums)) as "获得量"
from
(
select daily,role_id,gene_type,gene_star,count(distinct gene_id) as have_nums
   from (
  --截止每天每个玩家当前拥有的基因、基因等级
	select daily,role_id,gene_type,gene_id,gene_level,gene_star
	from
	(
	select daily,role_id,gene_type,gene_id,action,gene_level,gene_star,
			row_number() over(partition by role_id,gene_id,daily order by created_at desc)as rank
	FROM
	    (  
		     SELECT
				date_format( x, '%Y-%m-%d' ) daily,
				cast( to_unixtime ( x ) AS INTEGER ) daily_tag 
			FROM
				unnest (
				sequence ( cast ( '2021-03-08' AS date ), CURRENT_DATE, INTERVAL '1' DAY )) t ( x )
	    ) as t1 
	left join
	(

		select role_id,gene_type,gene_id,action,gene_level,gene_star,created_at,
				DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as gene_day
		from log_gene_change
	   where role_id in (select uuid from roles where is_internal=false and server_id!='1')
	   and gene_type in (790010000,790020000,790030000)
	) as t2 on 1=1
		 where gene_day<=daily 
	) as tmp1 where rank=1 and action not in ('5554','5558') --除去被扣除的基因
	) as tmp2 group by daily,role_id,gene_type,gene_star

) as a
left join (
select role_id,gene_type, case when gene_star=3 and level=2 then level
				when gene_star=4 and level=3 then level
				when gene_star=5 and level=4 then level
				when gene_star=6 and level=5 then level
				when gene_star=7 and level=6 then level
				when gene_star=8 and level=7 then level
				when gene_star=9 and level=8 then level
				when gene_star=10 and level=9 then level else 0 end as type,
				DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day,
				count(role_id) as nums 


from log_gene_change
where action='5550'
group by role_id,gene_type,case when gene_star=3 and level=2 then level
				when gene_star=4 and level=3 then level
				when gene_star=5 and level=4 then level
				when gene_star=6 and level=5 then level
				when gene_star=7 and level=6 then level
				when gene_star=8 and level=7 then level
				when gene_star=9 and level=8 then level
				when gene_star=10 and level=9 then level else 0 end,
				DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
)as b

on a.role_id=b.role_id and a.daily=b.act_day and a.gene_star=b.type and a.gene_type=b.gene_type
inner join roles on roles.uuid=a.role_id
left join (
	--基因获得
select role_id,gene_type,gene_star,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as get_day,
	count(distinct gene_id) as get_gene_nums
	from log_gene_change
	where action='1514'
	group by role_id,gene_star,gene_type,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
	) as  c on a.role_id=c.role_id and  a.daily=c.get_day and a.gene_star=c.gene_star and a.gene_type=c.gene_type
	
    cross join unnest(array[daily,'全部'] )as t (daily)
    cross join unnest(array[country,'全部'] )as t (country)
    cross join unnest(array[server_id,'全部'] )as t (server_id)
	
group by
t.daily,a.gene_type,a.gene_star,t.country,t.server_id