@动人 大佬，两个提数需求
1、需求：
log_activity_score表中活动id为2010701和2010601活动相关的玩家数据，两个活动分开拉取。
2、需求目的：
分析新服S15和S16的活动相关数据
3、筛选条件：
服务器：S14-S16
注册时间：UTC 10.21 00:00:00 - UTC 10.25 23:59:59
去内玩
4、数据格式
角色名称 国家 基地等级 积分变动 积分剩余量 对应变动时间 所属联盟 extend_1 extend_5
select t1.*,t2.country
from
(
select ac_id,role_id,role_name,server_id,base_level,nums,balance,
	DATE_FORMAT( from_unixtime( floor( created_at / 60 )* 60 ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%i:%s' )
						AS time,alliance_id,extend_1,extend_5
from log_activity_score
where server_id in ('S14','S15','S16')
and ac_id in ('2010701','2010601')
order by ac_id,role_id,created_at
) as t1 inner join
( select uuid ,country
from roles 
where is_internal=false
and created_at >=1634774400  and created_at<1635206400
) as t2 on t1.role_id=t2.uuid