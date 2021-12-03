1.需求：玩家pve、gve次数
2.筛选条件：
时间：9.13~10.19
3.数据格式：
日期，dau，唯一设备dau，新增注册人数，人均pve次数、人均gve次数 @小乔 










SELECT active_user.daily AS daily ,
	COALESCE ( role_create_num.val, 0 ) "新增注册人数",
	COALESCE( active_user.val, 0 ) DAU,
	COALESCE( active_device.val, 0 ) "唯一设备dau",
	COALESCE( pve.uv_pve, 0 ) "pve人数",
	COALESCE( pve.pv_pve, 0 ) "pve次数",
	COALESCE( pve.uv_gve, 0 ) "gve人数",
	COALESCE( pve.pv_gve, 0 ) "gve次数"


FROM
	(
	SELECT
		
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.role_id ) val 
	FROM
		log_login_role
	where role_id in (select uuid from roles where is_internal=false and server_id!='1')
	and created_at>=1631491200 and  created_at<1634688000
	  and month>=9
	GROUP BY
		
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_user
	left join
	(
	SELECT
		
		DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
		count( DISTINCT log_login_role.device_id ) val 
	FROM
		log_login_role
	where role_id in (select uuid from roles where is_internal=false and server_id!='1')
	and created_at>=1631491200 and  created_at<1634688000
	  and month>=9
	GROUP BY
		
	DATE_FORMAT( FROM_UNIXTIME( log_login_role.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
	active_device 
	ON 
	 active_device.daily = active_user.daily
	
  LEFT JOIN (
			SELECT
				
				DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily,
				count( DISTINCT roles.uuid ) val 
			FROM
				roles
				where is_internal=false and roles.server_id!='1'
				and created_at>=1631491200 and  created_at<1634688000

			GROUP BY
				
			DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' )) 
			role_create_num 
			ON 
			 role_create_num.daily = active_user.daily
left join
	( SELECT daily,
		count(distinct 
	 			if(step_id not in ('10001','10002','10003','10004','10005','10006','9501','9505',
	 				'9510','9515','9520','9525','9530') or (step_id='10002' and participant is  null),role_id,null)) as "uv_pve" ,
	 		count(distinct 
	 			if(step_id in ('10001','10003','10004','10005','10006','9501','9505',
	 				'9510','9515','9520','9525','9530') or (step_id='10002' and participant is not null),role_id,null)) as "uv_gve" ,
			
			count( 
	 			if(step_id not in ('10001','10002','10003','10004','10005','10006','9501','9505',
	 				'9510','9515','9520','9525','9530') or (step_id='10002' and participant is  null),role_id,null)) as  "pv_pve" ,
			count( 
	 			if(step_id in ('10001','10003','10004','10005','10006','9501','9505',
	 				'9510','9515','9520','9525','9530') or (step_id='10002' and participant is not null),role_id,null)) as  "pv_gve"

	 FROM( 
	 select role_id,step_id,json_extract(extend_3,'$.participant[0].roleid')  as participant
	 		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE '+00:00', '%Y-%m-%d' ) daily
	 from log_game_step
	 where step_type ='pve' 
	  and created_at>=1631491200 and  created_at<1634688000
	  and month>=9
	  
	  )as tmp
	 group by daily
	) as pve 
			ON 
			 pve.daily = active_user.daily
			
		ORDER BY
		daily DESC