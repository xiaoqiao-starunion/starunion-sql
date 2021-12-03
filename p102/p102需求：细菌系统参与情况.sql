需求：细菌系统参与情况
目的：了解新玩法参与情况
筛选：11.29-12.01
数据需求:
1.日期 付费分层 DAU 蚁后>=17级人数 变异菌丛>=6级人数 蚁后>=10级人数 拥有决斗坑道人数
2.日期 付费分层 决斗坑道挑战人数 决斗坑道挑战次数 挑战券购买人数 挑战券购买次数
3.日期 段位（小段位） 人数 蚁后平均等级 平均战力
4.付费分层 细菌类别 星级 等级 细菌数量
 @小乔 


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
 (  select  role_id,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day,
    row_number() over(partition by role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') order by created_at desc) as rank
    from log_logout
    where created_at>=1636934400 and created_at<1638403200
    and month>=11
    ) as tmp where rank=1

 union all

 select role_id,base_level,log_day --每天最后一条登录的等级，但可能只有登录没有登出
  from
 (  select  role_id,base_level,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as log_day,
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
     from(  select role_id,build_conf_id,build_level,created_at,
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