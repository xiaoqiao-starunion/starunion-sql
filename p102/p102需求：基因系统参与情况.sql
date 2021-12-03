需求：基因系统参与情况
目的：了解新系统玩家 参与程度
筛选：11.15-11.17
数据需求
1）日期 付费分层 DAU 蚁后>=13级人数dau 变异菌丛>=13级人数dau(蚁群变异池) 平均获得基因数量 平均强化基因次数(升级)
 获得基因人数 获得基因且变异菌丛>=13人数 
2）日期 付费分层 物品 获取途径 获取人数 获取次数 获取数量
（物品：遗传因子I,遗传因子II,遗传因子III,基因链,高等基因链）
（物品：遗传因子I,遗传因子II,遗传因子III,基因链,高等基因链，强击基因，剧毒基因，厚甲基因）
3）付费分层 基因类型 获得量 装备量 平均等级 平均星级
（基因类型：强击基因、剧毒基因、厚甲基因）


select log_day,user_type,count(distinct role_id) as dau,
				count(distinct if(base_level>=13,role_id,null)) as uv_level_13_more,
				count(distinct if(build_level>=13,role_id,null)) as uv_build_13_more,
				count(distinct get_gene_uid) as get_gene_uv,--获得基因人数
				count(distinct if(build_level>=13,get_gene_uid,null)) as uv_build_13_more_AND_get_gene,
				sum(get_gene_nums) as get_gene_nums,--获得基因数量
				sum(strong_gene_nums) as strong_gene_nums
from(
select t2.role_id,user_type,t2.log_day,t2.base_level,build_level,t4.role_id as get_gene_uid,get_gene_nums,strong_gene_nums
from
(
select uuid,
	case when paid <=0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false and server_id!='1'
) as t1
inner join
( --活跃玩家每天的最高等级
select role_id,log_day,max(base_level) as base_level 
from(
  select role_id,base_level,log_day --每天最后一条登出的等级，但可能只有登录没有登出
  from
 (	select  role_id,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day,
	row_number() over(partition by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') order by created_at desc) as rank
	from log_logout
	where created_at>=1636934400 and created_at<1638403200
	and month>=11
	) as tmp where rank=1

 union all

 select role_id,base_level,log_day --每天最后一条登录的等级，但可能只有登录没有登出
  from
 (	select  role_id,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day,
	row_number() over(partition by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') order by created_at desc) as rank
	from log_login_role
	where created_at>=1636934400 and created_at<1638403200
	and month>=11
	) as tmp where rank=1
 )as a group by role_id,log_day
) as t2 on t1.uuid=t2.role_id 


left join 
(  --截止每天，玩家解锁的变异菌丛的最大等级
select role_id,daily as act_day,build_level
from(
	select role_id,daily,build_level,row_number() over(partition by role_id,daily order by build_level desc) as rank
	from
   (  
		  SELECT
				date_format( x, '%Y-%m-%d' ) daily,
				cast( to_unixtime ( x ) AS INTEGER ) daily_tag 
			FROM
				unnest (
				sequence ( cast ( '2021-03-08' AS date ), CURRENT_DATE, INTERVAL '1' DAY )) t ( x )
	    ) as t1 
   left join
   ( select role_id,build_level,build_day
     from(	select role_id,build_conf_id,build_level,created_at,
   	        row_number() over(partition by role_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
   	        	order by build_level desc) as rank1,
   					DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as build_day
			from log_buildings
			where  action in ('6202','6201')
			and build_conf_id between 4094000 and 4094025
			)as tmp where rank1=1
   	) as t2 on 1=1
  where build_day<=daily
  ) as tmp where rank=1 and daily>='2021-11-15'
)as t3 on t2.role_id=t3.role_id and t2.log_day=t3.act_day and t1.uuid=t3.role_id
left join (
	select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as gene_day,
	count(distinct gene_id) as get_gene_nums
	from log_gene_change
	where  created_at>=1636934400 and created_at<1638403200
	and action='1514'
	group by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
 )as t4 on t2.role_id=t4.role_id and t2.log_day=t4.gene_day and t1.uuid=t4.role_id

left join (
	select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as gene_day,
	count(role_id) as strong_gene_nums
	from log_gene_change
	where  created_at>=1636934400 and created_at<1638403200
	and action='5551' --升级
	group by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
 )as t5 on t2.role_id=t5.role_id and t2.log_day=t5.gene_day and t1.uuid=t5.role_id

) as tmp group by log_day,user_type




需求二
select gene_day,user_type,action,item_id,count(distinct uuid) as uv,sum(pv) as pv,sum(nums) as nums
from
(
select uuid,
	case when paid <=0 then '未充值'
				when paid>0 and paid<=200 then '小R'
				when paid>200 and paid<=1000 then '中R'
				when paid>1000 then '大R'
				end as user_type
from roles 
where is_internal=false and server_id!='1'
) as t1
inner join
( select role_id,action,item_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as gene_day,
			sum(nums) as nums,count(role_id) as pv--次数
	from log_item
	where created_at>=1636934400 and created_at<1638403200
	and ((item_id between 204162 and 204167) --材料道具ID 
	or (item_id between 211001 and 211027) or item_id=217040)
	and nums>0
    group by role_id,action,item_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')
 )as t2 on t1.uuid=t2.role_id
group by user_type,action,item_id,gene_day





需求三
with user as
(
	select uuid,
		case when paid <=0 then '未充值'
					when paid>0 and paid<=200 then '小R'
					when paid>200 and paid<=1000 then '中R'
					when paid>1000 then '大R'
					end as user_type
	from roles 
	where is_internal=false and server_id!='1'
) 


select user_type,gene_type,sum(get_nums) as get_nums,
			sum(dress_nums) as dress_nums,
			sum(have_nums) as have_nums,
			sum(gene_levels)as gene_levels,
			sum(gene_stars) as gene_stars
from(
select uuid,user_type,if(t1.gene_type is not null,t1.gene_type,t2.gene_type) as gene_type,
	get_nums, dress_nums,have_nums,gene_levels,gene_stars

from user
left join
( select role_id,gene_type,count(if(action='1514',role_id,null)) as get_nums --获得量,该时间范围内gene_type是最全的
	from log_gene_change 
	where created_at>=1636934400 and created_at<1638403200
    group by role_id,gene_type
 )as t1 on  t1.role_id=user.uuid
left join
( select role_id,gene_type,count(distinct gene_id) as dress_nums --取玩家身上装备的基因
	from
	(select role_id,gene_type,gene_id,action,gene_position,row_number() over(partition by role_id,gene_id order by created_at desc) as rank
	from log_gene_change
	where  created_at<1638403200 
	) as tmp where rank=1 and action in ('5553','5556','5551','5550') and gene_position!=0
	group by role_id,gene_type
)as t2 on t1.gene_type=t2.gene_type and user.uuid=t2.role_id
left join( 
   select role_id,gene_type,count(distinct gene_id) as have_nums,sum(gene_level) as gene_levels,sum(gene_star) as gene_stars
   from (
  --每个玩家当前拥有的基因、基因等级
	select role_id,gene_type,gene_id,action,gene_level,gene_star
	from
	(
	select role_id,gene_type,gene_id,action,gene_level,gene_star,row_number() over(partition by role_id,gene_id order by created_at desc) as rank
	from log_gene_change
	where  created_at<1638403200 
	) as tmp1 where rank=1 and action not in ('5554','5558') --除去被扣除的基因
    ) as tmp2 group by role_id,gene_type

)as  t3 on user.uuid=t3.role_id and t1.gene_type=t3.gene_type 
where if(t1.gene_type is not null,t1.gene_type,t2.gene_type) is not null
) group by user_type,gene_type