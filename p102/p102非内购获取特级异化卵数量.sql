p102非内购获取特级异化卵数量
S242~S246在8月1日到9月8日，玩家每天非内购获取（免费）的特级异化卵数量 
3300内购，1257平台操作,4402邮件过期删除,5523通行证付费奖励,7300累计储值
表结构：服务器、日期、DAU、特级异化卵获取数量 @小乔 


--获得
select t1.server_id,log_day,dau,pay_nums,free_nums
from (
select server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as act_day,
		sum(if(action in ('3300','1257','4402','5523','7300') ,nums,0)) as pay_nums,
		sum(if(action not in ('3300','1257','4402','5523','7300') ,nums,0)) as free_nums
from log_item
where item_id=204023 and nums>0 
		and created_at >=1627776000 and created_at<1631145600 --改时间
		and role_id in (select uuid from roles where is_internal=false and server_id!='1')
group by server_id,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) 
) as t1
inner join(
select count(distinct role_id) as dau
		,server_id
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as log_day
from log_login_role
where role_id in (select uuid from roles where is_internal=false and server_id!='1')
		-- and created_at >=1627776000 and created_at<1631145600 --改时间
group by server_id
		,DATE_FORMAT( FROM_UNIXTIME(created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' )
) as t2 on t1.server_id=t2.server_id and log_day=act_day
