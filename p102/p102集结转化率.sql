p102集结转化率.sql
区服范围：242-245
数据结构：
开服第1天-30天，DAU，参与人数，发起集结数，集结成功数，集结取消原因和数量
 @小乔 

发起集结是记在LogMissions的，extendFiled.Extend1  =  2 的是发起集结时的打点记录
集结失败取消和成功都是记在 LogMassTroops里了

MassResult 的值：

	RALLY_SUC                       = 1 // 集结成功
	RALLY_DISBAND_PLAYER            = 2 // 玩家主动解散集结
	RALLY_DISBAND_FORCE             = 3 // 强制解散集结
	RALLY_DISBAND_INVALID_TARGET    = 4 // 集结目标错误
	RALLY_DISBAND_MEMBER_NOT_ENOUGH = 5 // 集结人数不足




select diff,log_day,
		dau,
		parti_uv as "参与集结人数",
		parti_pv as "参与集结次数",
		start_uv as "发起集结人数",
		start_pv as "发起集结次数",
		suc_uv as "集结成功人数",
		suc_pv as "集结成功次数",
		fail_1_uv as "1_集结失败人数",
		fail_1_pv as "1_集结失败次数",
		fail_2_uv as "2_集结失败人数",
		fail_2_pv as "2_集结失败次数",
		fail_3_uv as "3_集结失败人数",
		fail_3_pv as "3_集结失败次数",
		fail_4_uv as "4_集结失败人数",
		fail_4_pv as "4_集结失败次数"

from
(

   select count(distinct role_id) as dau,log_day,diff
	from
	(select role_id,server_id,open_day,log_day,
            DATE_DIFF (
                'day',
                DATE_PARSE ( open_day, '%Y-%m-%d' ),
            DATE_PARSE ( log_day, '%Y-%m-%d' )) AS diff
  from
  (
    select uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as open_day
  from servers
  where try_cast(uuid as integer) between 242 and 245
  ) t1
  inner join
  (select role_id,server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
    from log_login_role
    where role_id in (select uuid from roles where server_id!='1' and is_internal=false)
    group by role_id,server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
  ) as t2 on t1.uuid=t2.server_id
  ) as tmp group by log_day,diff
) as a 

left join 
( select count(distinct parti_uid) as parti_uv,
		sum(parti_pv) as parti_pv,
		parti_day
 from
 (
	select role_id  as parti_uid,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as parti_day--参与集结
		,count(role_id) as parti_pv --参与集结
	from log_missions
	where  category=7 and is_return=0 and try_cast(server_id as integer) between 242 and 245
    group by role_id,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as tmp group by parti_day
		
)as b on a.log_day=b.parti_day 
left join
(	select count(distinct start_uid) as start_uv,
		sum(start_pv) as start_pv,
		start_day
	from
	(
	select role_id  as  start_uid,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as start_day--发起集结
		,count(role_id) as start_pv --发起集结
	from log_missions
	where  category=7 and is_return=0 and Extend_1 = '2' and try_cast(server_id as integer) between 242 and 245
    group by role_id,
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
	)as tmp
	group by start_day

)as d on a.log_day=d.start_day
left join
(  
	select 
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day--集结
		,count(distinct if(mass_result=1,role_id,null))as suc_uv
		,count(distinct if(mass_result=2,role_id,null)) as fail_1_uv
		,count(distinct if(mass_result=3,role_id,null)) as fail_2_uv
		,count(distinct if(mass_result=4,role_id,null)) as fail_3_uv
		,count(distinct if(mass_result=5,role_id,null)) as fail_4_uv
		,sum(if(mass_result=1,1,0)) as suc_pv
		,sum(if(mass_result=2,1,0)) as fail_1_pv
		,sum(if(mass_result=3,1,0)) as fail_2_pv
		,sum(if(mass_result=4,1,0)) as fail_3_pv
		,sum(if(mass_result=5,1,0)) as fail_4_pv

	from log_mass_troops
	where  try_cast(server_id as integer) between 242 and 245
    group by 
		DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
        
	
	
) as c on a.log_day=c.act_day

