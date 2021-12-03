需求：联盟远征参与玩家战斗力
需求目的：优化匹配规则
筛选条件：10.22参与联盟远征的所有玩家
数据结构1：玩家ID，所属联盟ID，等级，战斗力
 @小乔 
select t1.role_id,alliance_id,base_level,t2.balance as power_1,t3.balance as power_2
 from
 (
 select  role_id,alliance_id,date_format(FROM_UNIXTIME(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d') daily,
      base_level,row_number() over(partition by role_id order by created_at) as rank1
    from log_missions
     where  log_missions.created_at>=1634860800 and log_missions.created_at<1634947200
                    and log_missions.extend_3 = 'battle_field' 
                    and role_id in (select uuid from roles where roles.server_id!='1' and is_internal=false)
   )as t1
   left join (
      select role_id,balance,row_number() over(partition by role_id order by created_at desc) as rank2
      from log_power_change
      where  created_at<=1634860800
          and (alliance_id is null or alliance_id='')
      )as t2 on t1.role_id=t2.role_id
   left join (
      select role_id,balance,row_number() over(partition by role_id order by created_at desc) as rank3
      from log_power_change
      where  created_at<=1634947200
          and (alliance_id is null or alliance_id='')
      )as t3 on t1.role_id=t3.role_id
   where rank2=1 and rank1=1  and rank3=1