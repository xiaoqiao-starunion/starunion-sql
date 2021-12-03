p102-开服第3和第4个月分天数据-每日任务完成情况：
筛选条件：
开服第3和第4个月分天数据
表结构：
第X天，活跃玩家，参与人数，完成档位
 @动人 可以只选3-5个服务器，怎么方便怎么来




with users as
( 
    select role_id,server_id,open_day,DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( log_day, '%Y-%m-%d' ))-60  as day_diff
        
    from
    (
        select role_id,server_id,open_day,log_day,
            DATE_DIFF (
                'day',
                DATE_PARSE ( open_day, '%Y-%m-%d' ),
            DATE_PARSE ( log_day, '%Y-%m-%d' )) AS diff
  from
  (
    select uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as open_day
  from servers
  ) t1
  inner join
  (select role_id,server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
    from log_login_role
    where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
    group by role_id,server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
    ) as t2 on t1.uuid=t2.server_id
  where DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( log_day, '%Y-%m-%d' ))>=60
        and DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( log_day, '%Y-%m-%d' ))<=90
  ) as t
    
)



SELECT log.server_id,log_uv,day_diff,level,task_uv
from
(
SELECT server_id,task_diff,level,count(distinct role_id) as task_uv
FROM
(

SELECT award.role_id,server_id,open_day,score,task_nums,task_day,DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( task_day, '%Y-%m-%d' ))-60 as task_diff,

                case when score >0 and score<30 then '0～30'
                            when score>=30 and score<70 then '30~70'
                            when score>=70 and score<120 then '70~120'
                            when score>=120 and score<180 then '120~180'
                            when score>=180 and score<260 then '180~260'
                            when score>=260 and score<340 then '260~340'
                            when score>=340 and score<420 then '340~420'
                            when score>=420 and score<500 then '420~500'
                            when score>=500  then '500+'
                            else 'NULL' end as level
                            
                            
FROM
(
SELECT role_id,task_day,max(balance) as score,count(action) as task_nums
FROM
(
SELECT role_id,balance,action--具体任务id
        ,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as task_day
FROM log_activity_score
where ac_id in ('1000100') --‘每日任务’
    and  role_id in (select uuid from roles where server_id!='1' and is_internal=false)
) as t
group by role_id,task_day
) as award
inner JOIN
(SELECT role_id,server_id,open_day,day_diff
FROM users
where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
)as users on users.role_id=award.role_id


where DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( task_day, '%Y-%m-%d' ))>=60
        and DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( task_day, '%Y-%m-%d' ))<=90


) as tmp group by server_id,task_diff,level

) as task 
inner join 
( select count(distinct role_id) as log_uv,server_id,day_diff
    from users
    group by server_id,day_diff
) as log
on log.day_diff=task.task_diff and log.server_id=task.server_id






--大号

with users as
( 
    select a.role_id,server_id,open_day,DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( log_day, '%Y-%m-%d' ))  as day_diff
        
    from
    (
        select role_id,server_id,open_day,log_day,
            DATE_DIFF (
                'day',
                DATE_PARSE ( open_day, '%Y-%m-%d' ),
            DATE_PARSE ( log_day, '%Y-%m-%d' )) AS diff
  from
  (
    select uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as open_day
  from servers
  ) t1
  inner join
  (select role_id,server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
    from log_login_role
    where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
    group by role_id,server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
    ) as t2 on t1.uuid=t2.server_id
  where DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( log_day, '%Y-%m-%d' ))>=0
        and DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( log_day, '%Y-%m-%d' ))<=120
  ) as a inner join 
    (
        select distinct role_id-- 大号 存在多对多情况
        from
        (
        select role_id ,device_id,base_level,row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        
        ) temp1 where rank=1
        
    ) b on a.role_id=b.role_id
    
)



SELECT log.server_id,log_uv,day_diff,level,task_uv
from
(
SELECT server_id,task_diff,level,count(distinct role_id) as task_uv
FROM
(

SELECT award.role_id,server_id,open_day,score,task_nums,task_day,DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( task_day, '%Y-%m-%d' )) as task_diff,

                case when score >0 and score<30 then '0～30'
                            when score>=30 and score<70 then '30~70'
                            when score>=70 and score<120 then '70~120'
                            when score>=120 and score<180 then '120~180'
                            when score>=180 and score<260 then '180~260'
                            when score>=260 and score<340 then '260~340'
                            when score>=340 and score<420 then '340~420'
                            when score>=420 and score<500 then '420~500'
                            when score>=500  then '500+'
                            else 'NULL' end as level
                            
                            
FROM
(
SELECT role_id,task_day,max(balance) as score,count(action) as task_nums
FROM
(
SELECT role_id,balance,action--具体任务id
        ,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as task_day
FROM log_activity_score
where ac_id in ('1000100') --‘每日任务’
    and  role_id in (select uuid from roles where server_id!='1' and is_internal=false)
) as t
group by role_id,task_day
) as award
inner JOIN
(SELECT role_id,server_id,open_day,day_diff
FROM users
where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
)as users on users.role_id=award.role_id
where DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( task_day, '%Y-%m-%d' ))>=0
        and DATE_DIFF (
                        'day',
                    DATE_PARSE ( open_day, '%Y-%m-%d' ),
                    DATE_PARSE ( task_day, '%Y-%m-%d' ))<=120


) as tmp group by server_id,task_diff,level

) as task 
inner join 
( select count(distinct role_id) as log_uv,server_id,day_diff
    from users
    group by server_id,day_diff
) as log
on log.day_diff=task.task_diff and log.server_id=task.server_id



