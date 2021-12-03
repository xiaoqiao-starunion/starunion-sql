需求：细胞培养情况
需求目的：研发数据支持
筛选条件：最近一周登录，且拥有七星细胞的玩家
数据结构：玩家ID，等级，充值金额，拥有七星细胞数量，最高七星细胞等级，小细胞核存量，大细胞核存量
 @小乔 

 select t2.role_id,base_level,paid,last_login_day,cell_level,t3.nums,t4.balance as small_balance,t5.balance as large_balance
 from
( select b.role_id,base_level,paid,last_login_day,cell_level
    from
(
 select uuid,base_level,paid,last_login_at,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
 from roles
 where last_login_at >= 1633651200
 and is_internal=false and server_id!='1'
 )as a
 inner join (
  select role_id,cell_level 
  from
  (
  select  role_id ,cell_level,row_number() over(partition by role_id  order by cell_level desc) as rank
  from log_hero_cell
  where  cell_star=7
  )as tmp where rank=1
 ) as b  on a.uuid=b.role_id
 ) as t2 

 inner join (
select role_id,
        -- count(distinct hero_cell_id)  as cell_nums,
        count(distinct extend_4) as nums
  from
  (
  select  distinct role_id,hero_cell_id,extend_4
  from log_hero_cell
  where  cell_star=7
  )as tmp
  group by role_id
 ) as t3
 on t3.role_id=t2.role_id
 left join 
 (  select role_id,balance
    from
    (
    select role_id,balance,row_number() over(partition by role_id  order by created_at desc) as rank
    from log_item
    where item_id=204158

    ) as tmp
    where rank=1

    ) as t4 
 on t2.role_id=t4.role_id
  left join 
 (  select role_id,balance
    from
    (
    select role_id,balance,row_number() over(partition by role_id  order by created_at desc) as rank
    from log_item
    where item_id=204159

    ) as tmp
    where rank=1

    ) as t5
 on t2.role_id=t5.role_id