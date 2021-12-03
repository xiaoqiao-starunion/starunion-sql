需求：Onestore注册玩家ID
目的：新渠道创建数验证
筛选：11.19 00:00~11.19 08:00 期间注册，商店来源为Onestore的玩家ID输出
输出：玩家ID @小乔 


select uuid,current_role
from accounts
where platform='2004' and created_at>=1637280000 and created_at<1637366400