需求：细胞培养情况
需求目的：研发数据支持
筛选条件：最近一周登录，且拥有七星细胞的玩家
数据结构：玩家ID，等级，充值金额，拥有七星细胞数量，最高七星细胞等级，小细胞核存量，大细胞核存量
 @小乔 

(
 select uuid,base_level,paid,last_login_at
 from roles
 where created_at >= 1633651200
 )as t1
 inner join (
  select role_id,cell_level 
  from
  (
  select  role_id ,cell_level,row_number() over(partition by role_id  order by cell_level desc) as rank
  from log_hero_cell
  where  cell_star=7
  )as tmp where rank=1
 ) as t2 
 on t1.uuid=t2.role_id

 inner join (
select role_id,count(distinct hero_cell_id)  as cell_nums,count(distinct extend_4) as nums
  from
  (
  select  distinct role_id,hero_cell_id,extend_4
  where  cell_star=7
  )as tmp

 )
 