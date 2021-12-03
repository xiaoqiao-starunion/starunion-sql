提数需求
1.需求：玩家水资源相关数据
2.需求目的：玩家水资源分析
3.筛选条件
玩家筛选条件：大于10级,在10.25-10.31都登录过的玩家,唯一设备id
4.数据结构
玩家id 服务器 等级 玩家创建日期 历史充值金额 日期 用途 变化数量(消耗为负数) 剩余数量
 @小乔 

 with major_users as 
(
    select uuid,server_id,reg_day,base_level,paid
    from
(
select uuid,paid,server_id,
        DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day 
        ,roles.base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles 
inner join 
    (select role_id,log_days
    from
    ( select  role_id,count(distinct DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) )as log_days 
    from log_login_role
    where  created_at>=1635120000 and created_at<1635724800
    group by role_id
    )as tmp
     where log_days=7 
    )as login on login.role_id=roles.uuid
    where is_internal=false and roles.server_id!='1'
          and base_level>=10

 ) as a
inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,
            row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
        
    ) b on a.uuid=b.role_id
)


select uuid,paid,base_level,server_id,reg_day,t2.act_day,balance_resource,balance_item,t1.action,use_nums
from major_users
inner join
(
select role_id,
        action,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day,
        sum(nums) as use_nums
from log_resource 
where nums<0 and created_at>=1635120000 and created_at<1635724800
and resource_id=3
 and month=10
group by role_id,action,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as t1 on major_users.uuid=t1.role_id
left join 
( select role_id,balance balance_resource,
    DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
    ,row_number() over(partition by role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) order by created_at desc) as rank
  from log_resource 
  where created_at>=1635120000 and created_at<1635724800 and resource_id=3
   and month=10
)as t2 on major_users.uuid=t2.role_id and t2.act_day=t1.act_day
left join 
(   select role_id,act_day,sum(balance) as balance_item
    from
    ( select role_id,
            case when item_id=200012 then balance*1000
                    when item_id=200013 then balance*10000
                    when item_id=200014 then balance*20000
                    when item_id=200098 then balance*30000
                    when item_id=200099 then balance*50000
                    when item_id=200100 then balance*150000
                    when item_id=200101 then balance*500000
                    when item_id=200102 then balance*1500000
                    end as balance,
        DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
        ,row_number() over(partition by role_id,item_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) order by created_at desc) as rank
      from log_item 
      where created_at>=1635120000 and created_at<1635724800 and item_id in (200012,200013,200014,200098,200099,200100,200101,200102)
      ) as tmp where rank=1
    group by role_id,act_day
)as t3 on major_users.uuid=t3.role_id and t3.act_day=t1.act_day
where rank=1



--获取
select uuid,paid,base_level,server_id,reg_day,t6.act_day,t6.action,get_nums
from major_users
inner join
(
    
select role_id,
        action,
        DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day,
        sum(nums) as get_nums
from log_resource 
where nums>0 and created_at>=1635120000 and created_at<1635724800
and resource_id=3
 and month=10
group by role_id,action,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )

union all

select role_id,action,act_day,sum(nums) as get_nums
from
    ( select role_id,action,
            case when item_id=200012 then nums*1000
                    when item_id=200013 then nums*10000
                    when item_id=200014 then nums*20000
                    when item_id=200098 then nums*30000
                    when item_id=200099 then nums*50000
                    when item_id=200100 then nums*150000
                    when item_id=200101 then nums*500000
                    when item_id=200102 then nums*1500000
                    end as nums,
        DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
      from log_item 
      where created_at>=1635120000 and created_at<1635724800 and item_id in (200012,200013,200014,200098,200099,200100,200101,200102)
      and nums>0
      ) as tmp 
    group by role_id,act_day,action

)as t6
on major_users.uuid=t6.role_id 
