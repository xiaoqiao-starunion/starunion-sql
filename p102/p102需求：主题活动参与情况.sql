需求：主题活动参与情况
目的：了解主题活动参与情况
筛选：
日期：11.09-11.15
服务器:S2-S435
数据需求:
1）付费分层 DAU 满足活动条件人数 参与人数 战令购买人数 参与进度分布
2）付费分层 参与人数 贝壳碎片获得数 贝壳碎片消耗数 等级分布（1~20）
3）付费分层 任务id 完成人数
4）付费分层 活跃天数（1~7天）
5）付费分层 贝壳获取途径 获取人数 获取次数 获取贝壳碎片量

with user as
(
	select uuid,
		case when paid <=0 then '未充值'
					when paid>0 and paid<=200 then '小R'
					when paid>200 and paid<=1000 then '中R'
					when paid>1000 then '大R'
					end as user_type
	from roles 
	where is_internal=false and server_id!='1'
	and try_cast(server_id as int) between 2 and 435
) 