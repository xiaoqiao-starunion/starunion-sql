蚂蚁日历
筛选条件：时间8.7-8.29
表结构：
日期，等级大于等于6级的活跃玩家（去重），点击日历的玩家数（去重），点击日历次数



SELECT  login_day,
				count(distinct login.device_id ) as login_uv,
				count(distinct calendar_btn_click_uid) as calendar_btn_click_uid,
				sum(calendar_btn_click_pv) as calendar_btn_click_pv
FROM
(
SELECT t1.device_id,login_day,base_level
FROM
(SELECT device_id,base_level
			,row_number() over(partition by  device_id order by base_level desc) as rank -- 活动期间最大等级
FROM log_logout
where created_at >= 1628294400 and created_at < 1630281600
)t1 inner join (
SELECT device_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as login_day
FROM log_login_role
where created_at >= 1628294400 and created_at < 1630281600
--and is_internal=FALSE
and server_id!='1'
group by device_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
)t2 on t1.device_id = t2.device_id
 where rank=1 and  base_level>=6

)login 
left join
(
SELECT device_id,base_level,
				if(step_id='calendar_btn_click',device_id,null) as calendar_btn_click_uid, -- 点击日历
				if(step_id='calendar_btn_click',1,0) as calendar_btn_click_pv, -- 点击日历
				DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
FROM log_game_step
where -- step_type='ant_colony_activity_calendar' -- 蚂蚁日历
			 step_id='calendar_btn_click'
			and created_at >= 1628294400 and created_at < 1630281600
)log_game_step
 on log_game_step.device_id=login.device_id and act_day=login_day
group by login_day





蚂蚁日历
筛选条件：时间8.7-8.29
表结构：去新玩家
日期，等级大于等于6级的活跃玩家（去重），点击日历的玩家数（去重），点击日历次数



SELECT  login_day,
				count(distinct login.device_id ) as login_uv,
				count(distinct calendar_btn_click_uid) as calendar_btn_click_uid,
				sum(calendar_btn_click_pv) as calendar_btn_click_pv
FROM
(
select a.*,reg_day
FROM
(	
	SELECT t1.device_id,role_id,login_day,base_level
	FROM
		(   SELECT device_id,role_id,base_level
						,row_number() over(partition by  device_id order by base_level desc) as rank -- 活动期间最大等级
			FROM log_logout
			where created_at >= 1628294400 and created_at < 1630281600
		)t1 
		inner join (
			SELECT device_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as login_day
			FROM log_login_role
			where created_at >= 1628294400 and created_at < 1630281600
			--and is_internal=FALSE
			and server_id!='1'
			group by device_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
		)t2 on t1.device_id = t2.device_id
		 where rank=1 and  base_level>=6
) as a
left join
(	SELECT uuid,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day
	from roles
	where created_at >= 1628294400 and created_at < 1630281600
	and is_internal=false and server_id!='1'
) as b
on a.role_id=b.uuid and a.login_day=b.reg_day
where b.uuid is null --去新
)login 

left join
(
SELECT device_id,base_level,
				if(step_id='calendar_btn_click',device_id,null) as calendar_btn_click_uid, -- 点击日历
				if(step_id='calendar_btn_click',1,0) as calendar_btn_click_pv, -- 点击日历
				DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day
FROM log_game_step
where -- step_type='ant_colony_activity_calendar' -- 蚂蚁日历
			 step_id='calendar_btn_click'
			and created_at >= 1628294400 and created_at < 1630281600
)log_game_step
 on log_game_step.device_id=login.device_id and act_day=login_day
group by login_day