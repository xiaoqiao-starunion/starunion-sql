需求：野怪相关资源存量
需求目的：分析野怪相关的参与数据
筛选条件：大于等于16级，最近一周活跃的玩家（9.20-9.26），按设备ID
数据结构：
玩家ID，区服，等级，历史充值金额，野怪卵存量（204116），各类资源存量，野怪饲料存量（204115），
登录天数，孵化野怪次数





with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day,paid
        from
(
select uuid,server_id,paid
        ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where  is_internal=false and server_id!='1'
and base_level>=16

 ) as a

inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        
        ) temp1 where rank=1
        
    ) b on a.uuid=b.role_id

inner join 
( select distinct role_id
	from log_login_role
	where created_at >= 1632096000  and created_at < 1632700800--9.20-26
 and year=2021 and month=9 
) as c on a.uuid=c.role_id
)



--道具资源

select major_users.uuid,server_id,reg_day,base_level,paid,log_days as "登录天数",nums as "孵化野怪次数",item_id, balance
from major_users
left join
( select  role_id,count( distinct DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d')) as log_days
	from log_login_role
	where year=2021 and  month>=9
		and created_at >= 1632096000  and created_at < 1632700800--9.20-26
	group by role_id
)
as t0 on major_users.uuid=t0.role_id
left join 
( 
	 select role_id,count(role_id) as nums
		from log_hero
		where action in ('4600')  and year=2021 and month>=9
		and created_at >= 1632096000  and created_at < 1632700800--9.20-26
		group by role_id
	
) as t1 on major_users.uuid=t1.role_id

left join
(	select role_id,item_id, balance
	from
	(
	SELECT role_id,item_id,balance,
			row_number() over(partition by role_id,item_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_item 
	where  created_at < 1632700800  --截止9.26
	and item_id in (204116,204115,
200000,
200001,
200002,
200003,
200004,
200005,
200006,
200007,
200008,
200009,
200010,
200011,
200015,
200016,
200017,
200018,
200019,
200020,
200021,
200022,
200023,
200024,
200025,
200026,
200039,
200040,
200041,
200042,
200043,
200044,
200045,
200046,200047,
200048,
200049,
200050,
200051,
200052,
200053,
200054,
200055,
200056,
200057,
200058,
200059,
200060,
200061,
200062,
200063,
200064,
200065,
200066,
200067,
200068,
200069,
200070,
200071,
200072,
200081,
200082,
200083,
200085,
200086,
200087,
200088,
200089,
200091,
200092,
200093,
200094,
200095,
200096,
200097,
200109,
200110,
200164,
200201,
200202,
200203,
204038,
204039,
204040,
204041,
204042,
204043,
204067)




	) as t where rank=1  
) as users  on major_users.uuid=users.role_id



--资源表资源
select major_users.uuid,
		resource_type, balance
from major_users
left join    
(	select role_id,resource_id, balance,case when resource_id=2 then '肉'
											when resource_id=4 then '植物'
											when resource_id=6 then '湿土'
											when resource_id=7 then '沙子'
											when resource_id=8 then '蜜露'
											end as resource_type

	from
	(
	SELECT role_id,resource_id,balance,
			row_number() over(partition by role_id,resource_id order by created_at desc ) as rank,
			DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
	FROM log_resource
	where created_at < 1632700800  --截止9.26
	) as tmp where rank=1  
) as t3
	on major_users.uuid=t3.role_id
	where resource_type is not null


--后续需要python处理



import pandas as pd
import numpy as np
import csv

pd1=pd.read_excel('/Users/mac1/Documents/星合/配置表/p102item资源配置.xlsx')
pd2=pd.read_csv('/Users/mac1/Documents/python_file_road/p102资源存量-明细20210929-道具表1.csv')


#df1=pd.DataFrame(pd1)
#df2=pd.DataFrame(pd2)
df3=pd.merge(pd1, pd2,how='left',left_on='ID',right_on='item_id') 
df3['nums_total']=df3['nums']*df3['balance']



#df3.info()


df4=df3.head(10)

print(df3.columns)

try:
   # df3.columns=[ 'type', 'role_id', 'item_id', 'nums_total']
    data1=df3.groupby(['uuid','server_id','reg_day','base_level','paid','登录天数','孵化野怪次数','type'])
    data2=data1.agg({'nums_total':['sum']})
    data3=data2.reset_index()
except:
    print('error')


data3.columns=['uuid','server_id','reg_day','base_level','paid','登录天数','孵化野怪次数','type','nums_total']

max(data3['nums_total'])
print(data3.type)





#道具+资源

pd3=pd.read_csv('/Users/mac1/Documents/python_file_road/p102资源存量-明细20210929-资源表1.csv')

df_merge=pd.merge(data3,pd3,left_on=['uuid','type'],right_on=['uuid','resource_type'],how='left')
print([column for column in df_merge])

df_merge['balance'].fillna(0, inplace=True)
df_merge['nums_total'].fillna(0, inplace=True)
df_merge['total']=df_merge['nums_total']+df_merge['balance']




#行转列
data4=df_merge[['uuid','server_id','reg_day','base_level','paid','登录天数','孵化野怪次数','type','total']]
data4=data4.set_index( ['uuid','server_id','reg_day','base_level','paid','登录天数','孵化野怪次数','type'])
data4=data4.unstack('type')
data4=data4.reset_index()


data4.to_csv('/Users/mac1/Documents/python_file_road/p102资源存量-result20210929-资源+道具.csv')