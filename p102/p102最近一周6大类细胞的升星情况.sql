/*
需求：最近一周6大类细胞的升星情况
需求目的：近期开了万圣节-细胞相关礼包，了解是否对细胞升星有所帮助提升
筛选条件：10.27-11.3（utc）、六大类细胞（甲壳、毒腺、口钳、蚁肢、腹囊、触角细胞）、星级
数据格式：分日、细胞类、星级、存量
*/


select cell_type,hero_cell_id, cell_star, '11.3' as cell_date, count(distinct extend_4) cell_cnt
from (select role_id, cell_type,hero_cell_id, 
	cell_star, extend_4, 
row_number() over(partition by role_id, hero_cell_id,extend_4 order by created_at desc) rn
from log_hero_cell
where  created_at < 1635984000 --11.3
and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
where rn = 1
group by  '11.3', hero_cell_id, cell_star,cell_type

union all

select cell_type,hero_cell_id, cell_star, '11.2' as cell_date, count(distinct extend_4) cell_cnt
from (select role_id, cell_type,hero_cell_id, 
	cell_star, extend_4, 
row_number() over(partition by role_id, hero_cell_id,extend_4 order by created_at desc) rn
from log_hero_cell
where  created_at < 1635897600 --11.2
and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
where rn = 1
group by  '11.2', hero_cell_id, cell_star,cell_type

union all
select cell_type,hero_cell_id, cell_star, '11.1' as cell_date, count(distinct extend_4) cell_cnt
from (select role_id, cell_type,hero_cell_id, 
	cell_star, extend_4, 
row_number() over(partition by role_id, hero_cell_id,extend_4 order by created_at desc) rn
from log_hero_cell
where  created_at < 1635811200 --11.1
and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
where rn = 1
group by  '11.1', hero_cell_id, cell_star,cell_type

union all


select cell_type,hero_cell_id, cell_star, '10.31' as cell_date, count(distinct extend_4) cell_cnt
from (select role_id, cell_type,hero_cell_id, 
	cell_star, extend_4, 
row_number() over(partition by role_id, hero_cell_id,extend_4 order by created_at desc) rn
from log_hero_cell
where  created_at < 1635724800 --10.31
and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
where rn = 1
group by  '10.31', hero_cell_id, cell_star,cell_type

union all

select cell_type,hero_cell_id, cell_star, '10.30' as cell_date, count(distinct extend_4) cell_cnt
from (select role_id, cell_type,hero_cell_id, 
	cell_star, extend_4, 
row_number() over(partition by role_id, hero_cell_id,extend_4 order by created_at desc) rn
from log_hero_cell
where  created_at < 1635638400 --10.30
and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
where rn = 1
group by  '10.30', hero_cell_id, cell_star,cell_type


union all

select cell_type,hero_cell_id, cell_star, '10.29' as cell_date, count(distinct extend_4) cell_cnt
from (select role_id, cell_type,hero_cell_id, 
	cell_star, extend_4, 
row_number() over(partition by role_id, hero_cell_id,extend_4 order by created_at desc) rn
from log_hero_cell
where  created_at < 1635552000 --10.29
and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
where rn = 1
group by  '10.29', hero_cell_id, cell_star,cell_type


union all

select cell_type,hero_cell_id, cell_star, '10.28' as cell_date, count(distinct extend_4) cell_cnt
from (select role_id, cell_type,hero_cell_id, 
	cell_star, extend_4, 
row_number() over(partition by role_id, hero_cell_id,extend_4 order by created_at desc) rn
from log_hero_cell
where  created_at < 1635465600 --10.28
and role_id in (select uuid from roles where server_id != '1' and is_internal = false)) t
where rn = 1
group by  '10.28', hero_cell_id, cell_star,cell_type