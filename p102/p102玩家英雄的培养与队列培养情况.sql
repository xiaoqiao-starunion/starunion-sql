提数需求
1.需求：玩家英雄的培养与队列培养情况
2.需求目的：分析新一批的英雄上线以后 玩家是否拓展多队列
3.筛选条件
最近7日活跃过的玩家(按当时10.14的要求)，等级13级以上的玩家，唯一设备id，玩家创建时间小于等于9.13
橙色特化蚁排除(1000122, 1000123, 1000124, 1000125, 1000126, 1000127)这六只 

时间 10.14和9.13 
4.数据结构(csv格式文件就好)
玩家id 日期 培养橙色特化蚁数（大于26级的橙色特化蚁） 初级协同作战解锁数 中级协同作战解锁数 高级协同作战解锁数 
10.14和9.13当天：特殊行军集结地上阵数 行军集结地1上阵数 行军集结地2上阵数 行军集结地3上阵数 全部行军集结地橙色特化蚁上阵总数
 @小乔 



with major_users as
(
    select uuid,server_id,reg_day,base_level,last_login_day,paid
    from
(
select uuid,paid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day 
        ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles 
where base_level>=13 and created_at<1631577600 --玩家创建时间小于等于9.13
and is_internal=false and server_id!='1'
 ) as a
inner join 
(
    select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,
            row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
    ) b on a.uuid=b.role_id
inner join(
    select distinct role_id
    from log_login_role
    where created_at >=1633564800 and created_at<1634256000 -- 活跃时间 10.7-10.14
    and year=2021 and month=10
) as c on a.uuid=c.role_id and b.role_id=c.role_id

) 


select uuid,server_id,reg_day,base_level,last_login_day,paid,hero_num as "培养橙色特化蚁数",primary,junior,senior,
        "特殊行军集结地上阵数","行军集结地1上阵数","行军集结地2上阵数","行军集结地3上阵数",hero_nums_type_4 as "全部行军集结地橙色特化蚁上阵总数"
from
(
select  uuid,server_id,reg_day,base_level,last_login_day,paid,
        sum(case when type='初级协同作战' and nums is not null then nums else 0 end) as primary,
        sum(case when type='中级协同作战' and nums is not null then nums else 0 end) as junior,
        sum(case when type='高级协同作战' and nums is not null then nums else 0 end) as senior

from major_users
left join
(
select role_id,
        '初级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7030100,7040100,7070100,7080100)
        and action in ('6401' ,'6202','1210')
        and created_at<1634256000 --10.15 00:00
group by role_id,
        '初级协同作战'


union

select role_id,
        '中级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7030800,7040800,7070800,7080800)
        and action in ('6401' ,'6202','1210')
        and created_at<1634256000 --10.15 00:00
group by role_id,
        '中级协同作战'

union

select role_id,
        '高级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7031500,7041500,7071500,7081500)
        and action in ('6401' ,'6202','1210')
        and created_at<1634256000 --10.15 00:00
group by role_id,
        '高级协同作战'


  
) as t on t.role_id=major_users.uuid     
group by  uuid,server_id,reg_day,base_level,last_login_day,paid

) as a  
left join(
    select role_id,count(distinct hero_id ) hero_num --培养的橙色特化蚁
    from log_hero
    where created_at<1634256000 --10.15 00:00
    and hero_type=4
    and hero_level>=26
    and hero_id not in (1000122, 1000123, 1000124, 1000125, 1000126, 1000127)
     group by role_id
) as d on a.uuid=d.role_id
left join 
(   select role_id,
    sum(case when troops_id='4' then max_hero_nums end) as "特殊行军集结地上阵数",
    sum(case when troops_id='1' then max_hero_nums end) as "行军集结地1上阵数",
    sum(case when troops_id='2' then max_hero_nums end) as "行军集结地2上阵数",
    sum(case when troops_id='3' then max_hero_nums end) as "行军集结地3上阵数"


    from(
            select  role_id,troops_id,max(hero_nums) as max_hero_nums
            from
            ( select tmp1.role_id,troops_id,created_at,count(distinct tmp1.hero_id) as hero_nums
            from 
            (
            select  
                    split(t2.heroid,',')[1] as role_id,
                    split(t2.heroid,',')[2] as hero_id,
                   troops_id,created_at
            from
                (   select role_id,troops_id,created_at
                        ,json_extract(army_id,'$.armymsg[0].heroid')  as hero1
                        ,json_extract(army_id,'$.armymsg[1].heroid')  as hero2
                        ,json_extract(army_id,'$.armymsg[2].heroid')  as hero3

                    from
                    (
                     select role_id
                        ,army_id ,troops_id,created_at       
                     from log_missions 
                     where created_at<1634256000 and created_at>1634169600 --10.14号当天
                     and month=10
                     and is_return=0
                    ) as t1 
            
                ) 
                CROSS JOIN UNNEST(array[concat(cast(role_id as varchar),',',cast(hero1 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero2 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero3 as varchar))]) AS t2 (heroid)
                group by split(t2.heroid,',')[1] ,split(t2.heroid,',')[2] ,troops_id,created_at
                having split(t2.heroid,',')[2]  is not null and split(t2.heroid,',')[2] !='0'
                )as tmp1
            where  hero_id not in ('1000122', '1000123', '1000124', '1000125', '1000126', '1000127')
            group by tmp1.role_id,troops_id,created_at
            ) as tmp2
                group by role_id,troops_id
) as tmp3 group by role_id
      
)as b on a.uuid=b.role_id

left join
( select tmp1.role_id,count(distinct tmp1.hero_id) as hero_nums_type_4
            from 
            (
            select  
                    split(t2.heroid,',')[1] as role_id,
                    split(t2.heroid,',')[2] as hero_id,
                   troops_id,created_at
            from
                (   select role_id,troops_id,created_at
                        ,json_extract(army_id,'$.armymsg[0].heroid')  as hero1
                        ,json_extract(army_id,'$.armymsg[1].heroid')  as hero2
                        ,json_extract(army_id,'$.armymsg[2].heroid')  as hero3

                    from
                    (
                     select role_id
                        ,army_id ,troops_id,created_at       
                     from log_missions 
                     where created_at<1634256000 and created_at>1634169600 --10.14号当天
                     and month=10
                     and is_return=0
                    ) as t1 
            
                ) 
                CROSS JOIN UNNEST(array[concat(cast(role_id as varchar),',',cast(hero1 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero2 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero3 as varchar))]) AS t2 (heroid)
                group by split(t2.heroid,',')[1] ,split(t2.heroid,',')[2] ,troops_id,created_at
                having split(t2.heroid,',')[2]  is not null and split(t2.heroid,',')[2] !='0'
                )as tmp1
            inner join(
                select hero_id,hero_type
                from log_hero
                where hero_type=4
                 group by hero_id,hero_type
            ) as tmp2 on cast(tmp1.hero_id as int)=tmp2.hero_id
            where  tmp1.hero_id not in ('1000122', '1000123', '1000124', '1000125', '1000126', '1000127')
            group by tmp1.role_id
)as c on a.uuid=c.role_id


--- 9.13号


with major_users as
(
    select uuid,server_id,reg_day,base_level,last_login_day,paid
    from
(
select uuid,paid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day 
        ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles 
where base_level>=13 and created_at<1631577600 --玩家创建时间小于等于9.13
and is_internal=false and server_id!='1'
 ) as a
inner join 
(
    select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,
            row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
    ) b on a.uuid=b.role_id
inner join(
    select distinct role_id
    from log_login_role
    where created_at >=1633564800 and created_at<1634256000 -- 活跃时间 10.7-10.14
    and year=2021 and month=10
) as c on a.uuid=c.role_id and b.role_id=c.role_id

) 


select uuid,server_id,reg_day,base_level,last_login_day,paid,hero_num as "培养橙色特化蚁数",primary,junior,senior,
        "特殊行军集结地上阵数","行军集结地1上阵数","行军集结地2上阵数","行军集结地3上阵数",hero_nums_type_4 as "全部行军集结地橙色特化蚁上阵总数"
from
(
select  uuid,server_id,reg_day,base_level,last_login_day,paid,
        sum(case when type='初级协同作战' and nums is not null then nums else 0 end) as primary,
        sum(case when type='中级协同作战' and nums is not null then nums else 0 end) as junior,
        sum(case when type='高级协同作战' and nums is not null then nums else 0 end) as senior

from major_users
left join
(
select role_id,
        '初级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7030100,7040100,7070100,7080100)
        and action in ('6401' ,'6202','1210')
        and created_at<1631577600 --9.14 00:00
group by role_id,
        '初级协同作战'


union

select role_id,
        '中级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7030800,7040800,7070800,7080800)
        and action in ('6401' ,'6202','1210')
        and created_at<1631577600 --9.14 00:00
group by role_id,
        '中级协同作战'

union

select role_id,
        '高级协同作战' as type,
        count(distinct tech_id) as nums

from log_tech 
where tech_id in (7031500,7041500,7071500,7081500)
        and action in ('6401' ,'6202','1210')
        and created_at<1631577600 --9.14 00:00
group by role_id,
        '高级协同作战'


  
) as t on t.role_id=major_users.uuid     
group by  uuid,server_id,reg_day,base_level,last_login_day,paid

) as a  
left join(
    select role_id,count(distinct hero_id ) hero_num --培养的橙色特化蚁
    from log_hero
    where  created_at<1631577600 --9.14 00:00
    and hero_type=4
    and hero_level>=26
    and hero_id not in (1000122, 1000123, 1000124, 1000125, 1000126, 1000127)
     group by role_id
) as d on a.uuid=d.role_id
left join 
(   select role_id,
    sum(case when troops_id='4' then max_hero_nums end) as "特殊行军集结地上阵数",
    sum(case when troops_id='1' then max_hero_nums end) as "行军集结地1上阵数",
    sum(case when troops_id='2' then max_hero_nums end) as "行军集结地2上阵数",
    sum(case when troops_id='3' then max_hero_nums end) as "行军集结地3上阵数"


    from(
            select  role_id,troops_id,max(hero_nums) as max_hero_nums
            from
            ( select tmp1.role_id,troops_id,created_at,count(distinct tmp1.hero_id) as hero_nums
            from 
            (
            select  
                    split(t2.heroid,',')[1] as role_id,
                    split(t2.heroid,',')[2] as hero_id,
                   troops_id,created_at
            from
                (   select role_id,troops_id,created_at
                        ,json_extract(army_id,'$.armymsg[0].heroid')  as hero1
                        ,json_extract(army_id,'$.armymsg[1].heroid')  as hero2
                        ,json_extract(army_id,'$.armymsg[2].heroid')  as hero3

                    from
                    (
                     select role_id
                        ,army_id ,troops_id,created_at       
                     from log_missions 
                     where created_at<1631577600 and created_at>1631491200 --9.13号当天
                     and month=9
                     and is_return=0
                    ) as t1 
            
                ) 
                CROSS JOIN UNNEST(array[concat(cast(role_id as varchar),',',cast(hero1 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero2 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero3 as varchar))]) AS t2 (heroid)
                group by split(t2.heroid,',')[1] ,split(t2.heroid,',')[2] ,troops_id,created_at
                having split(t2.heroid,',')[2]  is not null and split(t2.heroid,',')[2] !='0'
                )as tmp1
            where  hero_id not in ('1000122', '1000123', '1000124', '1000125', '1000126', '1000127')
            group by tmp1.role_id,troops_id,created_at
            ) as tmp2
                group by role_id,troops_id
) as tmp3 group by role_id
      
)as b on a.uuid=b.role_id

left join
( select tmp1.role_id,count(distinct tmp1.hero_id) as hero_nums_type_4
            from 
            (
            select  
                    split(t2.heroid,',')[1] as role_id,
                    split(t2.heroid,',')[2] as hero_id,
                   troops_id,created_at
            from
                (   select role_id,troops_id,created_at
                        ,json_extract(army_id,'$.armymsg[0].heroid')  as hero1
                        ,json_extract(army_id,'$.armymsg[1].heroid')  as hero2
                        ,json_extract(army_id,'$.armymsg[2].heroid')  as hero3

                    from
                    (
                     select role_id
                        ,army_id ,troops_id,created_at       
                     from log_missions 
                    where created_at<1631577600 and created_at>1631491200 --9.13号当天
                     and month=9
                     and is_return=0
                    ) as t1 
            
                ) 
                CROSS JOIN UNNEST(array[concat(cast(role_id as varchar),',',cast(hero1 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero2 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero3 as varchar))]) AS t2 (heroid)
                group by split(t2.heroid,',')[1] ,split(t2.heroid,',')[2] ,troops_id,created_at
                having split(t2.heroid,',')[2]  is not null and split(t2.heroid,',')[2] !='0'
                )as tmp1
            inner join(
                select hero_id,hero_type
                from log_hero
                where hero_type=4
                 group by hero_id,hero_type
            ) as tmp2 on cast(tmp1.hero_id as int)=tmp2.hero_id
            where  tmp1.hero_id not in ('1000122', '1000123', '1000124', '1000125', '1000126', '1000127')
            group by tmp1.role_id
)as c on a.uuid=c.role_id


  

