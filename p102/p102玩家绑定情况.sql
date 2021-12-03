p102玩家绑定情况.sql


--活跃
select log_date,t.device_os,count(distinct uuid) as dau,count(distinct if(log_date>bind_date,account_id,null)) as bind_dau 
from
(
select uuid,device_os,log_date,account_id,bind_date
from
    (
        select split(t1.uuid,':',3)[3] as uuid,device_os,log_date
        from
        (
         select accounts.uuid 
         from accounts inner join roles on roles.account_id=accounts.uuid
         where   is_internal=false and roles.server_id!='1'
        ) as t1
        inner join
        ( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
        from log_login_role
         where (created_at>=1638057600 and created_at<1638144000)--11.28 
         or  (created_at>=1636934400 and created_at<1637193600)--11.15-17
        )as t2 on t1.uuid=t2.acc_id
    )as a 
    left join
    (
        select account_id,bind_date
        from
        (
        select account_id,identity_type,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') bind_date
        from operation.operations_identities
        where is_delete=false and game_id in (2,5,6,7,8,9,10,11) and identity_type in (2,3,4)
        ) as tmp 
    ) as b on account_id=try_cast(uuid as int)
) as tmp
cross join unnest(array[device_os,'全部'] )as t (device_os)
group by log_date,t.device_os
having t.device_os in ('全部','IOS','Android')


--充值

 select log_date,t.device_os,count(distinct if(pay_date<log_date,uuid,null)) as dpu,
 count(distinct if(log_date>bind_date and pay_date<log_date,account_id,null)) as bind_dpu
 from ( 
    select split(uuid,':',3)[3] as uuid,device_os,pay_date,log_date
    from
    (
    select  uuid,pay_date
    from
    (
     select accounts.uuid 
     from accounts inner join roles on roles.account_id=accounts.uuid
     where   is_internal=false and roles.server_id!='1'
    ) as t1
    inner join
    ( select distinct account_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
    from payments
     where 
      is_paid=1
     and is_test=2
     and status=2
    )as t2 on t1.uuid=t2.account_id
    )as a 
    inner join
    ( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
     from log_login_role 
      where (created_at>=1638057600 and created_at<1638144000)--11.28 
         or  (created_at>=1636934400 and created_at<1637193600)--11.15-17
    )as b on a.uuid=b.acc_id

    ) as d1
    left join
    (
        select account_id,identity_type,bind_date
        from
        (
        select account_id,identity_type,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') bind_date
        from operation.operations_identities
        where is_delete=false and game_id in (2,5,6,7,8,9,10,11) and identity_type in (2,3,4)
        ) as tmp 
    ) as d2 on account_id=try_cast(uuid as int)


cross join unnest(array[device_os,'全部'] )as t (device_os)
group by log_date,t.device_os
having t.device_os in ('全部','IOS','Android')







去新活跃




select log_date,t.device_os,count(distinct if(reg_date<log_date,uuid,null)) as dau,
        count(distinct if(reg_date<log_date and log_date>bind_date,account_id,null)) as bind_dau 
from
(
select uuid,device_os,log_date,account_id,bind_date,reg_date
from
    (
        select split(t1.uuid,':',3)[3] as uuid,device_os,log_date,reg_date
        from
        (
         select accounts.uuid,DATE_FORMAT(FROM_UNIXTIME(roles.created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') reg_date
         from accounts inner join roles on roles.account_id=accounts.uuid
         where   is_internal=false and roles.server_id!='1'
        ) as t1
        inner join
        ( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
        from log_login_role
           where (created_at>=1638057600 and created_at<1638144000)--11.28 
         or  (created_at>=1636934400 and created_at<1637193600)--11.15-17
        )as t2 on t1.uuid=t2.acc_id
    )as a 
    left join
    (
        select account_id,identity_type,bind_date
        from
        (
        select account_id,identity_type,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') bind_date
        from operation.operations_identities
        where is_delete=false and game_id in (2,5,6,7,8,9,10,11) and identity_type in (2,3,4)
        ) as tmp 
    ) as b on account_id=try_cast(uuid as int)
) as tmp

cross join unnest(array[device_os,'全部'] )as t (device_os)
group by log_date,t.device_os
having t.device_os in ('全部','IOS','Android')


--去新充值

 select log_date,t.device_os,count(distinct if(reg_date<log_date and pay_date<log_date,uuid,null)) as dpu,
 count(distinct if(reg_date<log_date and log_date>bind_date and pay_date<log_date,account_id,null)) as bind_dpu
 from ( 
    select split(uuid,':',3)[3] as uuid,device_os,pay_date,log_date,reg_date
    from
    (
    select  uuid,pay_date,reg_date
    from
    (
     select accounts.uuid ,DATE_FORMAT(FROM_UNIXTIME(roles.created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') reg_date
     from accounts inner join roles on roles.account_id=accounts.uuid
     where   is_internal=false and roles.server_id!='1'
    ) as t1
    inner join
    ( select distinct account_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') pay_date
    from payments
     where 
      is_paid=1
     and is_test=2
     and status=2
    )as t2 on t1.uuid=t2.account_id
    )as a 
    inner join
    ( select distinct acc_id,device_os,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') log_date
     from log_login_role 
      where (created_at>=1638057600 and created_at<1638144000)--11.28 
         or  (created_at>=1636934400 and created_at<1637193600)--11.15-17
    )as b on a.uuid=b.acc_id

    ) as d1
    left join
    (
        select account_id,identity_type,bind_date
        from
        (
        select account_id,identity_type,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d') bind_date
        from operation.operations_identities
        where is_delete=false and game_id in (2,5,6,7,8,9,10,11) and identity_type in (2,3,4)
        ) as tmp 
    ) as d2 on account_id=try_cast(uuid as int)

cross join unnest(array[device_os,'全部'] )as t (device_os)
group by log_date,t.device_os
having t.device_os in ('全部','IOS','Android')



