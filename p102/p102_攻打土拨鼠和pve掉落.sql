p102_攻打土拨鼠和pve掉落.sql
提数需求（一）
09.05：“1617：攻打土拨鼠”行为下获得“204115: 野怪饲料”道具的玩家人数（去重）
服务器：2-282

提数需求（二）
09.07-09.13（分日）：“1609: PVE掉落”和“10001: 行军”行为下获得“204125: 贝壳碎片”道具的玩家人数（去重）
服务器：2-321

大致格式如下：
select count(distinct role_id) as uv,act_day
from
(
select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as act_day
from log_item
where action='1617' and item_id=204115
  and nums>0 and try_cast(server_id as integer) between 2 and 282
  and created_at > 1630800000 and created_at <1630886400
) group by act_day


select count(distinct role_id) as uv,act_day
from
(
select role_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE 'utc','%Y-%m-%d') as act_day
from log_item
where action in ('1609','10001') and item_id=204125
  and nums>0 and try_cast(server_id as integer) between 2 and 321
  and created_at > 1630972800 and created_at <1631577600
) group by act_day 