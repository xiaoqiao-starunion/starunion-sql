特化蚁培养情况20210914
注册7天以上，等级大于等于8级，最近2天内活跃玩家，最强队伍的3只特化蚁相关数据
玩家ID，服务器，国家，注册时间，最后一次培养特化蚁的时间，历史充值金额，当前等级，
行军集结地最大等级，橙色蚂蚁数量、橙色残壳余量、上阵数量、每只上阵蚂蚁等级和排序、
孢子存量，奇异蜜露存量，特化蚁A名称，等级，技能1等级，技能2等级。。。，特化蚁B名称，等级，技能1等级，技能2等级。。。，特化蚁C名称，等级，技能1等级，技能2等级。。。（未解锁的技能等级填0） @动人 

with users as
(
select uuid,server_id,country,paid,
	DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
	,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where created_at<1630886400 --注册时间小于9.6
    and last_login_at>=1631318400 --最后一次活跃时间>=9.11
    and is_internal=false and server_id!='1'
    and base_level>=8

 ) ,
hero as 
(
	select tmp1.role_id,tmp1.hero_id,hero_level
	from 
	(
	select  
	split(t2.heroid,',')[1] as role_id,split(t2.heroid,',')[2] as hero_id
	from
		(select role_id,mis_day
				,troops_id
				,json_extract(army_id,'$.armymsg[0].heroid')  as hero1
				,json_extract(army_id,'$.armymsg[1].heroid')  as hero2
				,json_extract(army_id,'$.armymsg[2].heroid')  as hero3

		from
		(
		 select role_id,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as mis_day,hero_info,army_id,beast_info ,troops_id,
		 		row_number() over(partition by role_id order by created_at desc) as rank 
		 from log_missions 
		 inner join users 
		    on users.uuid=log_missions.role_id
		 where troops_id='4'--特殊队列
		       and created_at>= 1631318400
		) as t1 where rank=1
		) CROSS JOIN UNNEST(array[concat(cast(role_id as varchar),',',cast(hero1 as varchar)),
		concat(cast(role_id as varchar),',',cast(hero2 as varchar)),
		concat(cast(role_id as varchar),',',cast(hero3 as varchar))]) AS t2 (heroid)
		)as tmp1
	inner join 
	(select role_id,hero_id,hero_level
		,row_number() over(partition by role_id,hero_id order by hero_level desc) as rank 
	from log_hero_skill
	) as tmp2 on tmp1.role_id=tmp2.role_id and cast(tmp1.hero_id as bigint)=tmp2.hero_id
	where rank=1

) ,


users_base as 
( select users.*,a1.days,a2.build_level,a3.balance,
			 a4.balance1,a4.balance2,a4.balance3,a4.balance4,a4.balance5,a4.balance6
	from users 
	inner join 
	(
		select role_id,days 
		from 
			(

			SELECT	role_id,
			        row_number() over(partition by role_id order by created_at desc) as rank,
					DATE_FORMAT( FROM_UNIXTIME( log_item.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) days					
			FROM log_item 
			WHERE	action = '1212' and nums <0  --培养特化蚁 特化矣经验消耗
			) t1
			where rank=1
	) as a1 on a1.role_id=users.uuid
	inner join
	(
		select role_id,build_conf_id,build_level 
		FROM
			(
			SELECT role_id,build_conf_id,build_level
							,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as days
							,row_number() over(partition by role_id order by build_level desc) as rank
			FROM log_buildings
			where build_conf_id between 4025000 and 4028025 --职业+行军集结地id
			) as t2
			where rank=1

	) as a2 on a2.role_id=users.uuid

	inner join
	(
		select role_id,balance
		FROM
		( SELECT role_id,balance
				,row_number() over(partition by role_id order by created_at desc) as rank
					
		FROM log_item 
		WHERE item_id = 204067 --包子
		)as t3 where rank=1 

	)as a3 on a3.role_id=users.uuid
    
    inner join
    ( 
    	
		select role_id,
    			sum(if (level='level1' ,balance,0)) as balance1,
    			sum(if (level='level2' ,balance,0)) as balance2,
    			sum(if (level='level3' ,balance,0)) as balance3,
    			sum(if (level='level4' ,balance,0)) as balance4,
    			sum(if (level='level5' ,balance,0)) as balance5,
    			sum(if (level='level6' ,balance,0))as balance6
		from
		(
		select role_id,level,balance,
				row_number() over(partition by role_id,level order by created_at desc) as rank
		from
		(
		SELECT	role_id,created_at,balance,
				case when  item_id=204038 then 'level1'
				 when  item_id=204039 then 'level2'
				 when  item_id=204040 then 'level3'
				 when  item_id=204041 then 'level4'
				 when  item_id=204042 then 'level5'
				 when  item_id=204043 then 'level6'
				end as level

		FROM log_item
		WHERE item_id between 204038 and 204043
				
		) as tmp
		) as t4 where rank=1
		group by role_id


    )as a4 on a4.role_id=users.uuid
),



--蚂蚁数量、残壳余量

hero_type4 as
(select *
from
(
	
		select role_id,hero_id,hero_type
		from log_hero

		where  action in ('1212','1511','2800','2801','2802','2900','2901','2902','2092',
			'2093','4100','5501','5503','5505','5513','5514','5515','5516','5517','5518',
			'5519','5520','5521','6004','5502','5504','5506')
		     and hero_type=4
		  	and  hero_type=4  and created_at>1631404800 --9.12至今
				group by role_id,hero_id,hero_type

  			
  
	union
	  
		select role_id,hero_id,hero_type
		from(
		select role_id,server_id,hero_id,hero_type,skill_id,skill_level 

		from log_hero_skill
		where action in ('2900','2901','2902')--英雄技能解锁、升级
	      and  hero_type=4  and created_at>1631404800 --9.12至今
			
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
				 where created_at>1631404800 --9.12至今
				 and is_return=0

				 
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
				where hero_type=4
				 group by hero_id,hero_type
			) as tmp2 on cast(tmp1.hero_id as int)=tmp2.hero_id
			 where tmp1.hero_id is not null and tmp1.hero_id!='0'
			 	group by tmp1.role_id,tmp1.hero_id,hero_type
		)
  
)as temp group by role_id,hero_id,hero_type

),
--残壳
slice as
(
	select role_id, balance as slice_balance
	from
	(
	SELECT role_id, balance,
			row_number() over(partition by role_id order by created_at desc ) as rank
	FROM log_item 
	where item_id=204071
	) as t where rank=1 



)


select users_base.*,hero_type4_nums,slice_balance,hero_nums,t3.hero_id,hero_rank,t3.hero_level,hero_type,skill_id,skill_level 
from users_base 
left join 
(select role_id,count(distinct hero_id) as hero_type4_nums
from	hero_type4
where hero_type=4
group by role_id
)as t1 on t1.role_id=users_base.uuid
left join slice on slice.role_id=users_base.uuid
inner join 
( select role_id,count(hero_id) as hero_nums
from hero
group by role_id
)as t2 on t2.role_id=users_base.uuid
inner join 
(select role_id,hero_id,hero_level,row_number() over(partition by role_id  order by hero_level desc)as hero_rank 
from hero)as t3
on t3.role_id=users_base.uuid
inner join
(
	select role_id,hero_id,hero_type,skill_id,skill_level 
	from(
	select role_id,hero_id,hero_type,skill_id,skill_level 
			,row_number() over(partition by role_id,hero_id,skill_id order by skill_level desc) as rank
	from log_hero_skill
	where action in ('2900','2901','2902')
	) as tmp
	where rank=1
) as skill
on t3.role_id=skill.role_id and cast (t3.hero_id as bigint)=skill.hero_id





import pandas as pd
import numpy as np
import csv

df2=pd.read_csv('/Users/mac1/Documents/python_file_road/p102_特化蚁培养-users.csv')
df3=pd.read_excel('/Users/mac1/Documents/星合/102/特化蚁ID-T-rank.xls')
df4=pd.read_csv('/Users/mac1/Documents/python_file_road/p102_特化蚁培养-明细20210914.csv')

df_merge=pd.merge(df3,df4,on=['hero_id','skill_id'],how='left')
print([column for column in df_merge])


data2=df_merge.set_index( ['uuid','server_id', 'country', 'paid', 'reg_day', 'base_level', 'last_login_day', 'days', 
                   'build_level','hero_type4_nums','slice_balance', 'balance', 'balance1', 'balance2', 
                      'balance3', 'balance4', 'balance5', 'balance6','hero_nums','hero_id','hero_rank','hero_level',
                        'hero_type','count','skill_index'])
data2=data2.unstack('skill_index')
data2=data2.reset_index()

data2.to_csv('/Users/mac1/Documents/python_file_road/p102_特化蚁培养情况-result20210914.csv')


