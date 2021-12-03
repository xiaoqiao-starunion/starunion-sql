数据需求
需求：采集异常玩家的采集数据
需求目的：根据异常数据进行补偿
筛选条件：指定玩家id
数据要求：
1.玩家蚁巢等级、语言
2.11月2日区战获得积分
3.11月2日区战领取奖励档位
4.11月2日18:30以前出发18:30以后返回的采集队伍的资源目标、及对应采集消耗的时间（一般不止一队）action_id可以标记往返
5.该次采集的前一次对相同资源点采集 消耗的时间和获取资源量


'16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'3188819',
'19891411',
'16121414',
'7861118',


双倍奖励

select distinct role_id,tech_id
from log_tech
where tech_id in (7091900,7090300,7090900) and action in('6401','6202')
and created_at<=1638316800
and role_id in ( '18548404',
'6865645',
'7384639',
'10873285',
'9193713',
'11635877',
'17221587',
'10873285',
'19160310',
'5510439',
'3119966',
'4764725',
'11635877',
'7601597',
'5510439',
'5571944',
'9248808')


--最强战区最大档位
select  role_id,max(award_node) as max_award_node
from log_activity_award
where ac_id = '1000300' and created_at>=1638230400 and created_at<1638316800
and role_id in 
( '18548404',
'6865645',
'7384639',
'10873285',
'9193713',
'11635877',
'17221587',
'10873285',
'19160310',
'5510439',
'3119966',
'4764725',
'11635877',
'7601597',
'5510439',
'5571944',
'9248808')
group by role_id





--本次采集信息
with base as
(select t1.role_id,t1.score,t2.max_award_node,t3.base_level,battle_id,get_time
from
(
select role_id,sum(nums) as score
from log_activity_score
where 
ac_id = '1000300' and created_at>=1635811200 and created_at<1635897600
and role_id in 
('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199'
)group by role_id
)as t1
left join
( select  role_id,max(award_node) as max_award_node
from log_activity_award
where 
ac_id = '1000300' and created_at>=1635811200 and created_at<1635897600
and role_id in 
('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199')
group by role_id
)as t2
on t1.role_id=t2.role_id
left join
(
   --本次采集的时间
      select role_id,base_level,battle_id,sum(get_time) as get_time
      from
      ( 
      select m1.base_level,m1.role_id,m1.battle_id,m1.action_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%i:%s') return_time,
      DATE_FORMAT(FROM_UNIXTIME(finish_time) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%i:%s') start_time,
      (created_at-finish_time)/60/2 as get_time
      from
      (
      select role_id,battle_id,action_id,finish_time,base_level
      from log_missions
      where category=3 and is_return=0
       and created_at<=1635849000 --出发时间
       and month=11
       and role_id in 
('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199')
       ) as m1
      inner join
      (select role_id,battle_id,created_at,action_id
      from log_missions
      where category=3 
       and is_return=1
       and created_at>1635849000 --返回出发时间
       and month=11
       and role_id in 
('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199')
   ) as m2 on m1.role_id=m2.role_id and m1.action_id=m2.action_id
   and m1.battle_id=m2.battle_id

   ) as tmp
   group by role_id,battle_id,base_level
  

)as t3 on t3.role_id=t1.role_id

) 



--上一次采集 存在一个问题，如果玩家拍三支队伍，但是时间是不一样的，取最后一条的话只能去到一只队伍的数据
--只能根据比例发

select t1.role_id,base_level,t1.battle_id,t1.action_id,last_get_time,nums
from
(
select role_id,base_level,battle_id,action_id --上次采集的action_id
from
(
select base.role_id,base.battle_id,d2.action_id,base_level,
finish_time,row_number() over(partition by d2.role_id order by finish_time desc) as rank --上一次采集
from base
inner join  
   (  select role_id,battle_id,finish_time,action_id --取同对象的上次采集的battle_id
      from
      (
      select role_id,battle_id,action_id,finish_time
      from log_missions
      where category=3 and is_return=1
       and finish_time<1635849000 --返回达到
       and month>=10
       and role_id in 
('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199')      ) as m1
      
   ) as d2 on base.role_id=d2.role_id and base.battle_id=d2.battle_id

) as tmp1 where rank=1
) as t1
 left join
 (
       --取采集时间
    select role_id,battle_id,action_id,sum(last_get_time) as last_get_time
      from
      (
   select m1.role_id,m1.battle_id,m1.action_id,DATE_FORMAT(FROM_UNIXTIME(created_at) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%i:%s') return_time,
   DATE_FORMAT(FROM_UNIXTIME(finish_time) AT TIME ZONE '+00:00', '%Y-%m-%d %H:%i:%s') start_time,
   (created_at-finish_time)/60/2 as last_get_time --上次采集的时间
   from
   (
         select role_id,battle_id,action_id,finish_time
         from log_missions
         where category=3 and is_return=0
         and role_id in 

('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199')        
          ) as m1
   inner join
            (select role_id,battle_id,action_id,created_at
            from log_missions
            where category=3 
             and is_return=1
             and role_id in 
         ('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199')  ) as m2 
   on m1.role_id=m2.role_id and m1.action_id=m2.action_id
   and m1.battle_id=m2.battle_id
   ) as tmp group by role_id,battle_id,action_id
   ) as t2 on t1.role_id=t2.role_id and t1.battle_id=t2.battle_id and t1.action_id=t2.action_id
left join
( select role_id,nums,action_id
   from log_resource
   where action='1518'
   and role_id in 
('16677019',
'8333692',
'9814207',
'9475694',
'99708025',
'12714067',
'6190992',
'4449273',
'3188819',
'8598275',
'8483783',
'8137559',
'8478502',
'11482907',
'9537424',
'10388233',
'7135628',
'19891411',
'17923472',
'12364957',
'2781869',
'7134013',
'8308245',
'2474465',
'16121414',
'15118033',
'5438330',
'5509466',
'4948025',
'2739546',
'5167253',
'4340411',
'7861118',
'5694367',
'14134339',
'8751199'
))as t3 on t3.role_id=t2.role_id
and t3.action_id=t2.action_id



