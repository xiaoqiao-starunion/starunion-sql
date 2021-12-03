提数需求
1.需求：玩家付费数据
2.需求目的：分析玩家付费结构，付费用户价值分层
3.筛选条件
付费玩家
4.数据结构(csv格式文件就好)
用户id 玩家等级 国家 最后充值时间 充值次数 充值总额 注册时间 最后登录时间 


select uuid,base_level,country,
		DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%i:%s' ) as "注册时间", 
		DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%i:%s' ) as "最后登录时间" ,
		DATE_FORMAT( FROM_UNIXTIME( paid_at ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%i:%s' ) as "最后充值时间" ,
		paid as "充值总额",paid_times as "充值次数"
	
	
from roles
where is_internal=false and server_id!='1'
	and paid>0

