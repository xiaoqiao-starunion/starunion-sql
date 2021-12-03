1.需求：有特级异化卵消耗行为玩家的付费情况
2.需求目的：分析双倍孵化活动
3.筛选条件：
时间10.30~11.1，有特级异化卵消耗行为的玩家
4.数据格式：
服务器，玩家id，特级异化卵消耗量，特级异化卵获得量，内购获取特级异化卵数量，活动期间储值金额，累计储值金额， 10月30日之前持有 和11月1日之后持有橙色特化蚁存量
 @小乔 

select t1.role_id,t1.server_id,use_nums,get_nums,pay_get_nums,pay_total1,pay_total2,pay_total3,amount1,amount2,amount3
from
 (select role_id,item_id,server_id,sum(nums) as use_nums --消耗量
 from log_item
 where item_id=204023
 and created_at>=1635552000 and created_at<1635811200
 and nums<0
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
 group by role_id,item_id,server_id
 ) as t1
left join
( select role_id,item_id,server_id,sum(nums) as get_nums --获取量
 from log_item
 where item_id=204023
 and created_at>=1635552000 and created_at<1635811200
 and nums>0
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
  group by role_id,item_id,server_id
)as  t2
on t1.role_id=t2.role_id
left join
( select role_id,item_id,server_id,sum(nums) as pay_get_nums --内购获取量
 from log_item
 where item_id=204023 and action='3300'
 and created_at>=1635552000 and created_at<1635811200
 and nums>0
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
  group by role_id,item_id,server_id

)as  t3
on t1.role_id=t3.role_id
left join
( select role_id,server_id,sum(price) as pay_total1 --活动期间充值金额
 from payments
 where 
  created_at>=1635552000 and created_at<1635811200
 and is_paid=1 and is_test=2 and status=2
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
  group by role_id,server_id
)as  t4
on t1.role_id=t4.role_id
left join
( select role_id,server_id,sum(price) as pay_total2 --截止活动结束的历史总充值金额
 from payments
 where 
  created_at<=1635811200 
 and is_paid=1 and is_test=2 and status=2
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
  group by role_id,server_id
)as  t5 on t1.role_id=t5.role_id
left join
( select role_id,server_id,sum(price) as pay_total3 --截止目前的历史总充值金额
 from payments
 where 
  is_paid=1 and is_test=2 and status=2
 and role_id in (select uuid from roles where is_internal=false and server_id!='1')
  group by role_id,server_id
)as  t9 on t1.role_id=t9.role_id
left join
( select role_id,sum(amount) as amount1 --10.30号前
   from (
    select role_id,t1.id,t1.level,t1.amount
    from 
    (select role_id,params,row_number() over(partition by role_id order by created_at desc) as rank
    from log_role_data
    where type=1 and month>=10
    and created_at<1635552000 
    ) as t0
    cross join unnest(cast(cast(json_parse(params) as array(json)) as array(row(Id INTEGER,level INTEGER,amount INTEGER)))) 
    as temp(t1)
    where rank=1
    )as tmp1 
   inner join (
        select distinct hero_id,hero_type
        from log_hero
        where hero_type=4
    )as tmp2 on tmp1.id=tmp2.hero_id
    group by role_id
)as t6 on t1.role_id=t6.role_id
left join
( select role_id,sum(amount) as amount2 --11.1号之后 第一条存量
    from(
    select role_id,t1.id,t1.level,t1.amount
    from 
    (select role_id,params,row_number() over(partition by role_id order by created_at ) as rank
    from log_role_data
    where type=1 and month>=10
    and created_at>1635811200 
    ) as t0
    cross join unnest(cast(cast(json_parse(params) as array(json)) as array(row(Id INTEGER,level INTEGER,amount INTEGER)))) 
    as temp(t1)
    where rank=1
    
    )as tmp1 
   inner join (
        select distinct hero_id,hero_type
        from log_hero
        where hero_type=4
    )as tmp2 on tmp1.id=tmp2.hero_id
    group by role_id
)as t7 on t1.role_id=t7.role_id
left join
( select role_id,sum(amount) as amount3 --11.1号之前最后一条
    from(
    select role_id,t1.id,t1.level,t1.amount
    from 
    (select role_id,params,row_number() over(partition by role_id order by created_at desc) as rank
    from log_role_data
    where type=1 and month>=10
    and created_at<=1635811200 
    ) as t0
    cross join unnest(cast(cast(json_parse(params) as array(json)) as array(row(Id INTEGER,level INTEGER,amount INTEGER)))) 
    as temp(t1)
    where rank=1
    
    )as tmp1 
   inner join (
        select distinct hero_id,hero_type
        from log_hero
        where hero_type=4
    )as tmp2 on tmp1.id=tmp2.hero_id
    group by role_id
)as t8 on t1.role_id=t8.role_id