1.需求
按以下步骤
提取11.8日，UTC 7:00前，以及utc 8:15后，玩家的通过率 
resource_decompression1_begin
resource_decompression1_end
check_update_begin
check_update_end
download_version_list
parse_version_list
resource_update_begin
resource_update_10-100per
resource_update_end
check_update_begin
prepare_first_launch_begin
game_on_start
game_on_sdk_login
game_on_login
login_world_success
prepare_first_launch_end
check_update_end
2.需求目的
排查是否更新后导致玩家登录问题
3.筛选条件

4.数据格式
类似新手强引漏斗通过率


时间段1： UTC 11.8 0:00~7:00
时间段2：UTC 11.8 7:03~10:24
时间段3:  UTC 11.8 10:26~24:00
4.数据格式

select step_id,count(distinct device_id) as uv
from log_game_step 
where 
 log_game_step.created_at>=1636329600 and log_game_step.created_at<1636354800 --8号7:00前
and step_id in ('resource_decompression1_begin',
'resource_decompression1_end',
'check_update_begin',
'check_update_end',
'download_version_list',
'parse_version_list',
'resource_update_begin',
'resource_update_10-100per',
'resource_update_end',
'check_update_begin',
'prepare_first_launch_begin',
'game_on_start',
'game_on_sdk_login',
'game_on_login',
'login_world_success',
'prepare_first_launch_end',
'check_update_end'
)
group by step_id


select step_id,count(distinct device_id) as uv
from log_game_step 
where 
 log_game_step.created_at>=1636354980 and log_game_step.created_at<=1636367040 --UTC 11.8 7:03~10:24
and step_id in ('resource_decompression1_begin',
'resource_decompression1_end',
'check_update_begin',
'check_update_end',
'download_version_list',
'parse_version_list',
'resource_update_begin',
'resource_update_10-100per',
'resource_update_end',
'check_update_begin',
'prepare_first_launch_begin',
'game_on_start',
'game_on_sdk_login',
'game_on_login',
'login_world_success',
'prepare_first_launch_end',
'check_update_end'
)
group by step_id



select step_id,count(distinct device_id) as uv
from log_game_step 
where 
 log_game_step.created_at>=1636367160 and log_game_step.created_at<1636416000 -- UTC 11.8 10:26~24:00
and step_id in ('resource_decompression1_begin',
'resource_decompression1_end',
'check_update_begin',
'check_update_end',
'download_version_list',
'parse_version_list',
'resource_update_begin',
'resource_update_10-100per',
'resource_update_end',
'check_update_begin',
'prepare_first_launch_begin',
'game_on_start',
'game_on_sdk_login',
'game_on_login',
'login_world_success',
'prepare_first_launch_end',
'check_update_end'
)
group by step_id