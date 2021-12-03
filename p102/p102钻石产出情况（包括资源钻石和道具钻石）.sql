p102钻石产出情况（包括资源钻石和道具钻石）.sql
筛选条件：注册时间大于等于1个月，等级大于等于8级，取设备ID下最大等级角色
用户分层：8.30-9.05 活跃的
type1：零充用户，在线时长0-30分钟(活跃每日平均在线时长 总在线时长/活跃天数)
type2：零充用户，在线时长30-90分钟
type3：零充用户，在线时长90分钟以上
type4：充值金额10美金以内，在线时长0-30分钟
type5：充值金额10美金以内，在线时长30-120分钟
type6：充值金额10美金以内，在线时长120分钟以上
type7：充值金额10-100美金，在线时长0-30分钟
type8：充值金额10-100美金，在线时长30-120分钟
type9：充值金额10-100美金，在线时长120分钟以上
type10：充值金额100-1000美金，在线时长0-120分钟
type12：充值金额100-1000美金，在线时长120+分钟以上
type13：充值金额1000美金以上，在线时长0-120分钟
type14：充值金额1000美金以上，在线时长120+分钟
表结构：
日期（8.30-9.5），用户类型（type1-15），用户数量，人均在线时长，在线时长中位数，
钻石获取途径，获取人数，获取数量，钻石获取中位数（该类型玩家在当日该获取途径中钻石的中值）


with major_users as
(
        select uuid,server_id,reg_day,base_level,last_login_day,paid
        from
(
select uuid,server_id,paid
        ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day ,base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles
where created_at < 1628553600 --注册时间<=8.9

    and base_level>=8
    and is_internal=false and server_id!='1'

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
	where created_at >= 1630281600 and created_at<1630886400 --8.30-9.5活跃的用户
) as c on a.uuid=c.role_id
),


user_type as 
(
--select count(distinct role_id) as uv,type
--from
--(
select role_id,reg_day,paid,online_time/online_days as online_time_per_day
		, case 
		  when paid=0 and (online_time/online_days) between 0 and 30 then 'type1'
		  when paid=0 and (online_time/online_days) between 30 and 90 then 'type2'
		  when paid=0 and (online_time/online_days)   >90 then 'type3'
		  when paid>0 and paid<10 and (online_time/online_days) between 0 and 30 then 'type4'
		  when paid>0 and paid<10 and (online_time/online_days) between 30 and 120 then 'type5'
		  when paid>0 and paid<10 and (online_time/online_days)   >120 then 'type6'
		  when paid>=10 and paid<100 and (online_time/online_days) between 0 and 30 then 'type7'
		  when paid>=10 and paid<100 and (online_time/online_days) between 30 and 120 then 'type8'
		  when paid>=10 and paid<100 and (online_time/online_days)   >120 then 'type9'
		  when paid>=100 and paid<1000 and (online_time/online_days) between 0 and 120 then 'type10'
		  when paid>=100 and paid<1000 and (online_time/online_days)   >120 then 'type11'
		  when paid>=1000  and (online_time/online_days) between 0 and 120 then 'type12'
		  when paid>=1000  and (online_time/online_days)   >120 then 'type13'
		  else '其他' end as type

from(
	select role_id,sum(secs)/60 as online_time
			,count(distinct DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )) as online_days
	from log_session
	group by role_id

) as online inner join major_users on major_users.uuid=online.role_id
--) as user_base group by type
)


select user_type.role_id,type,online_time_per_day,action_day,action,get_nums_all

from user_type 
left join ( --注意一下left和inner：一个是标签玩家，一个是有钻石获取记录的玩家
select role_id,action_day,action,sum(get_nums) as get_nums_all
from (
		select role_id,action_day,action,
				sum(nums) as get_nums
	    from
	   (	select role_id,item_id,action_day,action,
			case when item_id=200073 then nums*5
					when item_id=200074 then nums*10
					when item_id=200075 then nums*20
					when item_id=200076 then nums*50
					when item_id=200077 then nums*100
					when item_id=200078 then nums*200
					when item_id=200079 then nums*1000
					when item_id=200080 then nums*5000
					end as nums

		from
		(
		SELECT role_id,item_id,nums,action,
				DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
		FROM log_item 
		where item_id between 200073 and 200080
		  and nums>0 --获取
		 and month>=8
		 and created_at >= 1630281600 and created_at<1630886400 --8.30-9.5
		) as t 
		) as item group by role_id,action_day,action

   union all

	select role_id,action_day,action,get_nums
	from
	(	select  role_id,action_day,action,sum(nums)as get_nums

		from
		(
		SELECT role_id,resource_id,nums,action,
				DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as action_day
		FROM log_resource 
		where resource_id =1 and nums>0
			and created_at >= 1630281600 and created_at<1630886400

		) as t  group by role_id,action_day,action
	) as resource
) as tmp group by role_id,action_day,action

) as get on get.role_id=user_type.role_id


-----python

import pandas as pd
import numpy as np
import csv
file=pd.read_csv('/Users/mac1/Documents/python_file_road/钻石产出情况-明细.csv') #/Users/mac1/Documents/python_file_road
#玩家日志
df=pd.DataFrame(file)

#print(df)
num=[]

            
            
            
            
f1 = {'online_time_per_day':['mean','median']}
result_1 = df.groupby(['type']).agg(f1).reset_index()
result_1.columns = ['type', 'time_mean','time_median']
result_1.to_excel('/Users/mac1/Documents/python_file_road/钻石产出情况-online_time_per_day.xls')
            
f2 = {'get_nums_all':['mean','median']}
result_2 = df.groupby(['type','action','action_day']).agg(f2).reset_index()
result_2.columns = ['type','action','action_day', 'nums_mean','nums_median']
result_2.to_excel('/Users/mac1/Documents/python_file_road/钻石产出情况get_nums_all.xls')


df_merge=pd.merge(df,result_1,on=['type'],how='left')
df_merge2=pd.merge(df_merge,result_2,on=['type','action','action_day'],how='left')

df3 = df_merge2.groupby(['action_day','type','time_mean','time_median','action','nums_mean','nums_median'
                         ]).agg({'role_id':['count'],'get_nums_all':['sum']}).reset_index()

df3.to_excel('/Users/mac1/Documents/python_file_road/钻石产出情况result2.xls')
df3.columns=['action_day','type','time_mean','time_median','action','nums_mean','nums_median','get_uv','get_nums']
df4 = df_merge2.groupby(['type']).agg({'role_id':['nunique']}).reset_index()
df4.columns=['type','uv']

df_result=pd.merge(df4,df3,on=['type'],how='left')

df_result.to_excel('/Users/mac1/Documents/python_file_road/p102钻石产出情况20210910.xls')