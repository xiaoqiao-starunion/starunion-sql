需求1： 11级玩家的科技达成情况
筛选条件：
注册时间：3.8-9.25，服务器：2-398
玩家等级范围：11级
数据格式：
科技，完成该科技人数

需求2：11级玩家所拥有的特化蚁中，平均等级最高的橙色特化蚁信息
筛选条件：
注册时间：3.8-9.25，服务器：2-398
玩家等级范围：11级   特化蚁品质：橙色
数据格式：
特化蚁名称/该特化蚁平均等级/该特化蚁各技能平均等级

需求3：16级玩家所拥有的野怪中，平均品质最高的野怪信息
筛选条件：
注册时间：3.8-9.25，服务器：2-398
玩家等级范围：16级  
数据格式：
野怪名称/该野怪平均星级


select tech_id,tech_level,count(distinct uuid) as uv,
from
(
select uuid,base_level,tech_id,tech_level
from
	( select uuid,base_level
		from roles
		where base_level=11 and is_internal= false and server_id!='1'
	) as a
inner join (
	select role_id,tech_id,tech_level,
	row_number() over(partition by role_id,tech_id order by created_at desc) as rank
	from log_tech
	where year=2021
	and action in ('6401','6402')
) as b  on a.uuid=b.role_id
   where  rank=1
) as tmp
group by tech_id,tech_level


--特化蚁等级

select hero_id,hero_level,count(distinct uuid) as uv
from
(
	select uuid,base_level,hero_id,hero_level
	from
	( select uuid,base_level
		from roles
		where base_level=11 and is_internal= false and server_id!='1'
	) as t1
	inner join
	(
	SELECT	role_id,hero_type,hero_level,hero_id,
		row_number() over(partition by role_id,hero_id order by created_at desc) as rank 
	FROM log_hero 
	WHERE
			hero_id between 1000101 and 1000168 --特化蚁id
			and hero_type=4 -- 1234 绿蓝紫橙
			
	) as t2
	on t1.uuid=t2.role_id 
	where rank=1 
) as a group by hero_id,hero_level

--技能等级分布
select count(distinct role_id) as uv,hero_id,hero_type,skill_id,skill_level
	from
	( select uuid,base_level
		from roles
		where base_level=11 and is_internal= false and server_id!='1'
	) as t1
	inner join
	(
	select role_id ,hero_id,hero_type,skill_id,skill_level
	from(
	select role_id,hero_id,hero_type,skill_id,skill_level ,
			row_number() over(partition by role_id,hero_id,skill_id order by skill_level desc) as rank 
	from log_hero_skill
	where action in ('2900','2901','2902')--英雄技能解锁、升级
      and  hero_type=4
	) as tmp where rank=1
	) as t2 
    on t1.uuid=t2.role_id
	group by hero_id,hero_type,skill_id,skill_level

--野怪星级

select hero_id,hero_star,count(distinct uuid) as uv
from
(
	select uuid,base_level,hero_id,hero_star
	from
	( select uuid,base_level
		from roles
		where base_level=16 and is_internal= false and server_id!='1'
	) as t1
	inner join
	(
	SELECT	role_id,hero_type,hero_star,hero_id,
		row_number() over(partition by role_id,hero_id order by created_at desc) as rank 
	FROM log_hero 
	WHERE
			hero_id in(72001000,72002000,72003000,72004000,72005000,72006000,72007000,72008000,
72009000,72010000,72011000,72012000,72013000)
			
			
	) as t2
	on t1.uuid=t2.role_id 
	where rank=1 
) as a group by hero_id,hero_star