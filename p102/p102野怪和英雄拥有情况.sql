提数需求一
1.需求：解锁野怪玩家，拥有各星级野怪情况
2.需求目的：分析玩家野怪持有情况分布
3.筛选条件：
时间：开服到现在，服务器：全服 当前等级>=16
4.数据格式：
服务器，野怪星级，野怪数量，人数



select server_id,hero_type,hero_star,hero_nums,count(distinct role_id) as uv
from
(
select role_id,server_id,hero_type,hero_star,count(distinct hero_id) as hero_nums
from
(
  select role_id,server_id,hero_id,hero_type,hero_star,action
  from
    ( select role_id,log_hero.server_id,hero_id,hero_type,hero_star,action,roles.base_level,
        row_number() over(partition by role_id,hero_id order by log_hero.created_at desc) as rank
        from log_hero inner join roles on roles.uuid=log_hero.role_id
        where 
         try_cast(hero_id as int) between 72001000 and 72013000 
         and is_internal=false and roles.server_id!='1'
         and roles.base_level>=16
    )as t1
   where rank=1
) as t2 group by role_id,server_id,hero_type,hero_star
) as t3 group by server_id,hero_type,hero_star,hero_nums



提数需求二
1.需求：解锁野怪玩家，拥有橙蚁数量情况
2.需求目的：分析玩家橙蚁持有情况分布
3.筛选条件：
时间：开服至今，服务器：全服
4.数据格式：（表头）
服务器，橙蚁数量，人数
 @小乔 

select server_id,hero_type,hero_nums,count(distinct role_id) as uv
from
(
select t2.role_id,t2.server_id,hero_type,count(distinct hero_id) as hero_nums
from
(
  select role_id,server_id,hero_id,hero_type,action
  from
    ( select role_id,log_hero.server_id,hero_id,hero_type,hero_star,action,
        row_number() over(partition by role_id,hero_id order by log_hero.created_at desc) as rank
        from log_hero inner join roles on roles.uuid=log_hero.role_id
        where 
         try_cast(hero_id as int) between 1000101 and 1000177
         and is_internal=false and roles.server_id!='1'
         and hero_type=4  
    )as t1
   where rank=1
) as t2 
inner join(
    select role_id,server_id
  from
    ( select distinct role_id,log_hero.server_id,roles.base_level
        from log_hero inner join roles on roles.uuid=log_hero.role_id
        where 
         try_cast(hero_id as int) between 72001000 and 72013000 
         and is_internal=false and roles.server_id!='1'
         and roles.base_level>=16
    )as t1
    ) as users --有野怪的人
on t2.role_id=users.role_id
group by t2.role_id,t2.server_id,hero_type
) as t3 group by server_id,hero_type,hero_nums








select server_id,hero_nums,count(distinct role_id) as uv
from
(
select count(distinct hero_id) as hero_nums,hero.role_id,server_id
from
(
    
        select role_id,hero_id,hero_type
        from log_hero

        where  action in ('1212','1511','2800','2801','2802','2900','2901','2902','2092',
            '2093','4100','5501','5503','5505','5513','5514','5515','5516','5517','5518',
            '5519','5520','5521','6004','5502','5504','5506')
             and hero_type=4 and created_at>1632787200
            and month>=9
                group by role_id,hero_id,hero_type

    union
      
        select role_id,hero_id,hero_type
        from(
        select role_id,server_id,hero_id,hero_type,skill_id,skill_level 

        from log_hero_skill
        where action in ('2900','2901','2902')--英雄技能解锁、升级
          and  hero_type=4  and created_at>1632787200 --9.28至今
        and month>=9
        ) as tmp

        group by role_id,hero_id,hero_type
    
        
  
    union
          select role_id,cast(hero_id as int) hero_id,hero_type
          from   
        (
            select tmp1.role_id,tmp1.hero_id,hero_type
            from 
            (
            select  
                    split(t2.heroid,',')[1] as role_id,
                    split(t2.heroid,',')[2] as hero_id
            from
                (select role_id
                        ,json_extract(army_id,'$.armymsg[0].heroid')  as hero1
                        ,json_extract(army_id,'$.armymsg[1].heroid')  as hero2
                        ,json_extract(army_id,'$.armymsg[2].heroid')  as hero3

                from
                (
                 select role_id
                    ,army_id        
                 from log_missions 
                 where created_at>1632787200 
                 and is_return=0 and month>=9

                 
                ) as t1 
            
                ) 
                CROSS JOIN UNNEST(array[concat(cast(role_id as varchar),',',cast(hero1 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero2 as varchar)),
                concat(cast(role_id as varchar),',',cast(hero3 as varchar))]) AS t2 (heroid)
                group by split(t2.heroid,',')[1] ,split(t2.heroid,',')[2] 
                having split(t2.heroid,',')[2]  is not null and split(t2.heroid,',')[2] !='0'
            )as tmp1
            inner join(
                select hero_id,hero_type
                from log_hero
                where hero_type=4 and month>=3
                 group by hero_id,hero_type
            ) as tmp2 on cast(tmp1.hero_id as int)=tmp2.hero_id
             where tmp1.hero_id is not null and tmp1.hero_id!='0'
                group by tmp1.role_id,tmp1.hero_id,hero_type
        )
  
)as hero inner join(
    select role_id,server_id
  from
    ( select distinct role_id,log_hero.server_id,roles.base_level
        from log_hero inner join roles on roles.uuid=log_hero.role_id
        where 
         try_cast(hero_id as int) between 72001000 and 72013000 
         and is_internal=false and roles.server_id!='1'
         and roles.base_level>=16
    )as t1
    ) as users --有野怪的人
on hero.role_id=users.role_id
group by role_id,server_id

) as tmp group by hero_nums,server_id