p102联盟GVE.sql：20210909
日期：8.30-9.3，按设备ID
表结构1：
满足活动玩家数(活动期间活跃 且16级且开服的服务器)，进入活动界面玩家数，完成挑战的人数（挑战25级小怪且有胜负结果的）、参与打野怪的次数
表结构2：
区服，联盟野怪boss（首领野怪，5个）,召唤次数，击败次数，参与玩家数
表结构3：
玩家ID，区服，等级，战力，注册时间，开启活动时间（以选择档位或者第一次击杀活动野怪（小怪）为准），
所选难度档位=野怪类型（1-5个难度）每个玩家只能选一次，时间、最高击败等级，参与打野怪的次数和战胜次数
 @动人 


 玩家挑战小怪
表:ApiLogGameStep
StepType : gve_monster
StepId: 怪的id
Extend1: 是否胜利
Extend2: 活动uuid 改为战斗ID


玩家挑战boss
表:ApiLogGameStep
StepType : gve_boss
StepId: boss的id
Extend1: 是否胜利
Extend2: 活动uuid 改为战斗ID
Extend3.参与集结的玩家
Extend4.记录召唤BOSS的玩家，参与击杀boss的玩家以及最终战斗结果      我这边当时就只打点了这两个   别的打点  你找小江对一下

25级小怪 step_id 可集结打25级
22025 22050 22075 22100 22125 



关于联盟gve打点方案：
位置log_game_step 
记录每一次集结的发起者、参与者、小怪/boss ID、 胜负结果
格式：其他基本信息、role_id(发起者)、extend_3:（json）参与集结的玩家id（除去发起者）、extend_4:所有参与玩家的队伍详情
extend_3 =  {"participant":[{"roleid":8221671},{"roleid":8221672},{"roleid":8221673}]}

20210908之后联盟gve打点有更改，以后取数以新的为准

with major_users as
(
    select uuid,server_id,power,reg_day,base_level,last_login_day,paid
    from
(
select uuid,paid,logout.server_id,power,
        DATE_FORMAT( FROM_UNIXTIME( roles.created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as reg_day 
        ,logout.base_level
        ,DATE_FORMAT( FROM_UNIXTIME( last_login_at ) AT TIME ZONE 'UTC', '%Y-%m-%d' ) as last_login_day
from roles 
inner join 
(select role_id,server_id,base_level,row_number() over(partition by role_id order by created_at desc) as rank
from log_logout
where  created_at>=1630281600 and  created_at<1630713600  --9.03
)as logout on logout.role_id=roles.uuid
where is_internal=false and roles.server_id!='1'
       and rank=1 --活动结束前最后一次登出等级   
 ) as a
inner join 
    (
        select distinct role_id -- 大号
        from
        (
        select role_id ,device_id,base_level,
            row_number() over(partition by device_id order by base_level desc) as rank
        from log_login_role
        ) temp1 where rank=1
        
    ) b on a.uuid=b.role_id
    where base_level>=16 and try_cast(server_id as integer) between 2 and 223 --参与活动的条件
)



表结构1：
满足活动玩家数(活动期间活跃 且16级且开服的服务器)，进入活动界面玩家数，完成挑战的人数（挑战25级小怪且有胜负结果的）、参与打野怪的次数
select count(distinct major_users.uuid) as uv--满足活动玩家数
        ,count(distinct join_activity.role_id) as join_uv--进入活动界面的玩家
        ,count(distinct monster.role_id) as monster_uv--参与挑战小怪的玩家
        ,count(distinct monster.finish_uid) as finish_uv--挑战25级小怪的玩家
        ,sum(pv) as pv--参与挑战野怪次数
from  major_users
left join 
(select  distinct  split(split(role_id,',')[1],':')[2] as role_id --进入活动界面,
from log_game_step 
where step_type in ('trun_to_activity_gve')
   and log_game_step.created_at>=1630281600 and log_game_step.created_at<1630713600
)as join_activity on major_users.uuid=join_activity.role_id

left join 
(
    select role_id,count(role_id) as pv,--参与挑战野怪次数
            if(step_id in ('22025', '22050', '22075', '22100', '22125'),role_id,null) as finish_uid

from
(
select role_id,step_id,Extend_1 --胜负
from log_game_step
where step_type in ('gve_monster')
    and created_at<1630713600 and created_at>=1630281600
)as t group by role_id ,if(step_id in ('22025', '22050', '22075', '22100', '22125'),role_id,null)
) as monster on monster.role_id=major_users.uuid




表结构2：
区服，联盟野怪boss（首领野怪，5个）,召唤次数，击败次数，参与玩家数


select t.server_id,step_id,
    count(role_id) as pv,count(if( extend_1='1' ,role_id,null)) as suc_pv,count(distinct role_id) as uv
from
(
select role_id,log_game_step.server_id,step_id,Extend_1
from log_game_step 
where step_type in ('gve_boss')
and created_at<1630713600 and created_at>=1630281600
)as t 
inner join major_users on major_users.uuid=t.role_id
group by t.server_id,step_id



表结构3：
玩家ID，区服，等级，战力，注册时间，开启活动时间（以选择档位或者第一次击杀活动野怪（小怪）为准），
所选难度档位=野怪类型（1-5个难度）每个玩家只能选一次，时间、最高击败等级，参与打野怪的次数和战胜次数

select uuid,server_id,base_level,power,reg_day,first_day,type,high_suc_id,act_day,pv,suc_pv
from major_users
inner join 
(   select t1.role_id,t1.first_day,t2.type,t4.high_suc_id,t4.act_day,t3.pv,t3.suc_pv
    from
    (
        select role_id,first_day      
        from
        (
        select role_id,step_id
                ,DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%I:%S' ) as first_day 
                ,row_number() over(partition by role_id order by created_at) as rank
        from log_game_step
        where step_type in ('gve_monster')
            and created_at<1630713600 and created_at>=1630281600
        )as first where rank=1 --第1次击杀小怪的时间
    ) as t1
    left join (
        select role_id
            ,case when try_cast(step_id as integer) between 22001 and 22025 then '瓢虫'
             when try_cast(step_id as integer) between 22026 and 22050 then '食蚜蝇'
             when try_cast(step_id as integer) between 22051 and 22075 then '寄生蜂'
             when try_cast(step_id as integer) between 22076 and 22100 then '蟹蛛'
             when try_cast(step_id as integer) between 22101 and 22125 then '草蛉'
             end as type
    from log_game_step
    where step_type in ('gve_monster')
        and created_at<1630713600 and created_at>=1630281600
        group by role_id
            ,case when try_cast(step_id as integer) between 22001 and 22025 then '瓢虫'
             when try_cast(step_id as integer) between 22026 and 22050 then '食蚜蝇'
             when try_cast(step_id as integer) between 22051 and 22075 then '寄生蜂'
             when try_cast(step_id as integer) between 22076 and 22100 then '蟹蛛'
             when try_cast(step_id as integer) between 22101 and 22125 then '草蛉'
             end 
        ) as t2 -- 玩家选取的类型
    on t1.role_id=t2.role_id
    left join (
        select role_id,step_id as high_suc_id, 
            DATE_FORMAT( FROM_UNIXTIME( created_at ) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%I:%S' ) as act_day 
            ,row_number() over(partition by role_id order by  try_cast(step_id as integer) desc) as rank1
            ,case when try_cast(step_id as integer) between 22001 and 22025 then '瓢虫'
             when try_cast(step_id as integer) between 22026 and 22050 then '食蚜蝇'
             when try_cast(step_id as integer) between 22051 and 22075 then '寄生蜂'
             when try_cast(step_id as integer) between 22076 and 22100 then '蟹蛛'
             when try_cast(step_id as integer) between 22101 and 22125 then '草蛉'
             end as type
            
    from log_game_step
    where step_type in ('gve_monster') and extend_1='1'
        and created_at<1630713600 and created_at>=1630281600
        
        ) as t4 -- 最大战胜等级
    on t1.role_id=t4.role_id

    left join (

        select role_id,count(role_id) as pv --参与次数
                ,count(if(extend_1='1' , role_id,null)) as suc_pv --胜利次数
        from log_game_step
        where step_type in ('gve_monster')
            and created_at<1630713600 and created_at>=1630281600
        group by role_id
        )as t3 on t3.role_id=t1.role_id
   where rank1=1 or t4.role_id is null
) as monster on monster.role_id=major_users.uuid



