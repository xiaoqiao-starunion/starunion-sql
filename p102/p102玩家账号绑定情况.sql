提数需求
1.需求：玩家账号绑定情况
2.需求目的：验收账号绑定活动效果
3.筛选条件
范围：全服     / 时间：截止UTC2021-10-29 08:00:00的绑定情况，以及截止UTC2021-11-1 00:00:00的绑定情况
4.数据结构
直接取10/28和10/31号当天的dau与dpu数据好了
然后绑定人数是要取DAU范围里的，以前那些绑定了但弃游的玩家不算



更新下11.7和11.14的数据
更新 11.18 11.19

日期-活跃&支付-玩家id

select distinct split(uuid,':',3)[3] as uuid,split(account_id,':',3)[3] account_id,a.device_os,log_date
from(
select  uuid,device_os,log_date
from
(
 select accounts.uuid 
 from accounts inner join roles on roles.account_id=accounts.uuid
 where   is_internal=false and roles.server_id!='1'
) as t1
inner join
( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
from log_login_role
 where (created_at>=1637193600 and created_at<1637280000)--11.18 

)as t2 on t1.uuid=t2.acc_id
) as a
left join
(
 select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1637193600  --11.18  00:00
	 and is_paid=1
	 and is_test=2
	 and status=2
	
) as b on a.uuid=b.account_id

union all

select distinct split(uuid,':',3)[3] as uuid,split(account_id,':',3)[3] account_id,a.device_os,log_date
from(
select  uuid,device_os,log_date
from
(
 select accounts.uuid 
 from accounts inner join roles on roles.account_id=accounts.uuid
 where   is_internal=false and roles.server_id!='1'
) as t1
inner join
( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
from log_login_role
 where  (created_at>=1637452800 and created_at<1637539200)--11.21
)as t2 on t1.uuid=t2.acc_id
) as a
left join
(
 select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1637452800  
	 and is_paid=1
	 and is_test=2
	 and status=2
	
) as b on a.uuid=b.account_id







日期-分设备-活跃

	select count(distinct uuid) as uv,t.device_os,t.log_date
	from
	(
	select split(t1.uuid,':',3) as uuid,device_os,log_date
	from
	(
	 select accounts.uuid 
	 from accounts inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	) as t1
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	from log_login_role
	 where (created_at>=1637193600 and created_at<1637280000)--11.18 
	 or  (created_at>=1637452800 and created_at<1637539200)--11.21
	)as t2 on t1.uuid=t2.acc_id
	)as a 
	cross join unnest(array[log_date,'全部'] )as t (log_date)
    cross join unnest(array[device_os,'全部'] )as t (device_os)
    
    group by t.log_date,t.device_os







--付费活跃



select d1.log_date,dpu,device_os,uv
from
(
	select count(distinct uuid) as dpu,log_date
	from
	(
	select  uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts 
	 inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1637193600 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role
	where (created_at>=1637193600 and created_at<1637280000)--11.18 
	
    )as b on a.uuid=b.acc_id

	group by log_date


)as d1
left join
(
	select count(distinct uuid) as uv,a.device_os,log_date
	from
	(
	select uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1637193600 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role 
	 where (created_at>=1637193600 and created_at<1637280000)--11.18 
    )as b on a.uuid=b.acc_id
	group by log_date,a.device_os
)as d2
on d1.log_date=d2.log_date

union all


select d1.log_date,dpu,device_os,uv
from
(
	select count(distinct uuid) as dpu,log_date
	from
	(
	select  uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts 
	 inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1637452800 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
   inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role
	where  (created_at>=1637452800 and created_at<1637539200)--11.21
	
    )as b on a.uuid=b.acc_id

	group by log_date


)as d1
left join
(
	select count(distinct uuid) as uv,a.device_os,log_date
	from
	(
	select  uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1637452800 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role
	 where 
	  (created_at>=1637452800 and created_at<1637539200)--11.21
    )as b on a.uuid=b.acc_id
	group by log_date,a.device_os
)as d2
on d1.log_date=d2.log_date



--去新



select distinct split(uuid,':',3)[3] as uuid,split(account_id,':',3)[3] account_id,a.device_os,log_date
from(
select  uuid,device_os,log_date
from
(
 select accounts.uuid 
 from accounts inner join roles on roles.account_id=accounts.uuid
 where   is_internal=false and roles.server_id!='1'
 and roles.created_at<1635292800 --去掉10.27号注册的用户
) as t1
inner join
( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
from log_login_role
 where  (created_at>=1635379200 and created_at<1635465600)--10.28 

)as t2 on t1.uuid=t2.acc_id
) as a
left join
(
 select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1635379200  --11.18 00:00
	 and is_paid=1
	 and is_test=2
	 and status=2
	
) as b on a.uuid=b.account_id

union all

select distinct split(uuid,':',3)[3] as uuid,split(account_id,':',3)[3] account_id,a.device_os,log_date
from(
select  uuid,device_os,log_date
from
(
 select accounts.uuid 
 from accounts inner join roles on roles.account_id=accounts.uuid
 where   is_internal=false and roles.server_id!='1'
  and roles.created_at<1635552000 --去掉30号注册的用户
) as t1
inner join
( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
from log_login_role
 where   (created_at>=1635638400 and created_at<1635724800)--10.31
)as t2 on t1.uuid=t2.acc_id 
) as a
left join
(
 select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1635638400  
	 and is_paid=1
	 and is_test=2
	 and status=2
	
) as b on a.uuid=b.account_id







日期-分设备-活跃
select d1.log_date,dau,device_os,uv
from
(
	select count(distinct uuid) as dau,log_date
	from
	(
	select split(t1.uuid,':',3) as uuid,device_os,log_date
	from
	(
	 select accounts.uuid 
	 from accounts 
	 inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	 and roles.created_at<1635292800 --去掉10.27号注册的用户
	) as t1
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	from log_login_role
	 where  (created_at>=1635379200 and created_at<1635465600)--10.28 
	)as t2 on t1.uuid=t2.acc_id
	)as a 
	group by log_date
)as d1
left join
(
	select count(distinct uuid) as uv,device_os,log_date
	from
	(
	select split(t1.uuid,':',3) as uuid,device_os,log_date
	from
	(
	 select accounts.uuid 
	 from accounts inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	  and roles.created_at<1635292800 --去掉10.27号注册的用户
	) as t1
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	from log_login_role
	 where  (created_at>=1635379200 and created_at<1635465600)--10.28 
	)as t2 on t1.uuid=t2.acc_id
	)as a group by log_date,device_os
)as d2
on d1.log_date=d2.log_date

union all

select d1.log_date,dau,device_os,uv
from
(
	select count(distinct uuid) as dau,log_date
	from
	(
	select split(t1.uuid,':',3) as uuid,device_os,log_date
	from
	(
	 select accounts.uuid 
	 from accounts 
	 inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	   and roles.created_at<1635552000 --去掉10.30号注册的用户
	) as t1
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	from log_login_role
	 where 
	    (created_at>=1635638400 and created_at<1635724800)--10.31
	)as t2 on t1.uuid=t2.acc_id
	)as a 
	group by log_date
)as d1
left join
(
	select count(distinct uuid) as uv,device_os,log_date
	from
	(
	select split(t1.uuid,':',3) as uuid,device_os,log_date
	from
	(
	 select accounts.uuid 
	 from accounts inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	   and roles.created_at<1635552000 --去掉10.30号注册的用户
	) as t1
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	from log_login_role
	 where
	  (created_at>=1635638400 and created_at<1635724800)--10.31
	)as t2 on t1.uuid=t2.acc_id
	)as a group by log_date,device_os
)as d2
on d1.log_date=d2.log_date





--付费活跃



select d1.log_date,dpu,device_os,uv
from
(
	select count(distinct uuid) as dpu,log_date
	from
	(
	select  uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts 
	 inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	 and roles.created_at<1635292800 --去掉10.27号注册的用户
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1635379200 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role
	 where (created_at>=1635379200 and created_at<1635465600)--10.28 
	
    )as b on a.uuid=b.acc_id

	group by log_date


)as d1
left join
(
	select count(distinct uuid) as uv,a.device_os,log_date
	from
	(
	select uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	 and roles.created_at<1635292800 --去掉10.27号注册的用户
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1635379200 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role 
	 where (created_at>=1635379200 and created_at<1635465600)--10.28 
    )as b on a.uuid=b.acc_id
	group by log_date,a.device_os
)as d2
on d1.log_date=d2.log_date

union all


select d1.log_date,dpu,device_os,uv
from
(
	select count(distinct uuid) as dpu,log_date
	from
	(
	select  uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts 
	 inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	 and roles.created_at<1635552000 --去掉10.30号注册的用户
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1635638400 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
   inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role
	where   (created_at>=1635638400 and created_at<1635724800)--10.31
	
    )as b on a.uuid=b.acc_id

	group by log_date


)as d1
left join
(
	select count(distinct uuid) as uv,a.device_os,log_date
	from
	(
	select  uuid,device_os,pay_date
	from
	(
	 select accounts.uuid 
	 from accounts inner join roles on roles.account_id=accounts.uuid
	 where   is_internal=false and roles.server_id!='1'
	 and roles.created_at<1635552000 --去掉10.30号注册的用户
	) as t1
	inner join
	( select distinct account_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
	from payments
	 where created_at<=1635638400 
	 and is_paid=1
	 and is_test=2
	 and status=2
	)as t2 on t1.uuid=t2.account_id
	)as a 
	inner join
	( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
	 from log_login_role
	 where 
	   (created_at>=1635638400 and created_at<1635724800)--10.31
    )as b on a.uuid=b.acc_id
	group by log_date,a.device_os
)as d2
on d1.log_date=d2.log_date


