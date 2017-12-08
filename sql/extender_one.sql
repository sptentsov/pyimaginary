


exec VK.ext.e1_extend_groups 
    @run_time = '2017-11-25'
  , @basic_group = 144657300 --imaginary
  --, @cnt_users_in_group_threshold = 1000
  --, @cnt_basic_users_threshold = 5
  --, @cnt_output_groups = 1000

--declare @run_time datetime2(3) = getdate()
exec VK.ext.e1_extend_groups 
    @basic_group = 144657300 --imaginary
  --, @run_time = @run_time

select * 
from vk.ext.groups
order by rank() over (partition by run_time order by k desc)
  , run_time


--  delete from vk.ext.groups

;with basic_users_at_time_point_rn as
(
select 
    action_dt
  , group_id
  , [user_id]
  , action_type
  , rn = row_number() over (partition by [user_id] order by action_dt desc)
from fact.groups_growth
where group_id = 144657300
  and action_dt < '2017-11-25'
)
, basic_users_at_time_point as
(
select [user_id]
from basic_users_at_time_point_rn
where rn = 1
  and action_type != 'exit'
)
, their_groups_at_time_point_rn as
(
select
    ug.[user_id]
  , group_id = ug.subscribed_on
  , ug.action_type
  , rn = row_number() over (partition by ug.[user_id], ug.subscribed_on order by ug.action_dt desc)
from 
  fact.users_groups as ug
  join basic_users_at_time_point as bu --just to filter
    on bu.[user_id] = ug.[user_id]
where ug.action_dt < '2017-11-25'
  and ug.is_group = 1
)
, groups_popularity_at_time_point as
(
select 
    group_id
  , cnt_basic_users = count(*)
from their_groups_at_time_point_rn
where rn = 1
  and action_type != 'exit'
group by group_id
)
, fact_groups_info_at_time_point_rn as
(
select 
    group_id
  , cnt_users = new_members_count
  , name = new_name
  , rn = row_number() over (partition by group_id order by action_dt desc)
from fact.groups_info
where action_dt < '2017-11-25'
)
, fact_groups_info_at_time_point as
(
select 
    group_id
  , cnt_users
  , name
from fact_groups_info_at_time_point_rn
where rn = 1
)
, final_stats_rn as
(
select
    p.group_id
  , p.cnt_basic_users
  , i.cnt_users
  , i.name
  , k = p.cnt_basic_users * 1. / i.cnt_users
  , rn = row_number() over (order by p.cnt_basic_users * 1. / i.cnt_users desc)
from
  groups_popularity_at_time_point as p
  join fact_groups_info_at_time_point as i
    on p.group_id = i.group_id
where i.cnt_users > 1000 
  and p.cnt_basic_users > 5
  and p.group_id != 144657300
)
select group_id, cnt_basic_users, cnt_users, name, k
from final_stats_rn
where rn <= 1000
order by rn


select * 
from groups_popularity_at_time_point
order by iif(cnt_users != 0, cnt_basic_users * 1. / cnt_users, 0) desc


select *
from [ext].[e1_vw_basic_users] 

select top 100 * from fact.users_groups order by action_dt
select top 100 * from dim.users_groups order by created_dt_precise


select top 100 * from fact.groups_growth order by action_dt
select top 100 * from dim.groups_members order by created_dt_precise


select top 100 * from fact.groups_info order by action_dt
select top 100 * from dim.groups order by created_dt_precise

select distinct action_dt from fact.groups_growth order by action_dt
select distinct action_dt from fact.users_groups order by action_dt
select distinct action_dt from fact.groups_info order by action_dt
select distinct created_dt_precise from dim.groups order by created_dt_precise



select top 100 * from dim.groups where updated_dt_precise = created_dt_precise order by updated_dt_precise desc

select count(*) from dim.groups_members where group_id = 144657300

select top 100 * from fact.users_groups where user_id = 25705	and subscribed_on = 23485910


select * from fact.groups_info where action_dt = '2017-11-25 02:54:30.743' or group_id = 109933399 or group_id = 144657300
select * from fact.groups_info where group_id = 144657300
select * from dim.groups where  group_id = 109933399



/*


-----наглым хаком заносим инициализацию
--генерацию фактов сделал '2017-11-25 02:54:30.743' на имажинари и МП, через 5 мин - полный репроцессинг
--поэтому всё, что created_at < '2017-11-25', то заносим как факт инициализации

;with get_first_group_params_rn as
(
select *
  , rn = row_number() over (partition by group_id order by action_dt)
from fact.groups_info
where action_dt > '2017-11-25'
  and old_name is not null --группа была до появления фактов
)
, get_first_group_params as
(
select
    group_id 
  , name = old_name
  , members_count = old_members_count
  --, action_dt, new_members_count, new_name
from get_first_group_params_rn
where rn = 1
)
insert into fact.groups_info
(action_dt, group_id, old_name, new_name, old_members_count, new_members_count)
select
    action_dt = g.created_dt_precise
  , g.group_id
  , old_name = null
  , new_name = isnull(f.name, g.name)
  , old_members_count = null
  , new_members_count = isnull(f.members_count, g.members_count)
  --, *
from 
  dim.groups as g
  left join get_first_group_params as f
    on g.group_id  = f.group_id
where g.created_dt_precise < '2017-11-25'
order by iif(f.name != g.name, 0, 1)

select * from fact.groups_info where group_id = 119762339
select * from dim.groups where group_id = 119762339
select * from fact.groups_info where group_id = 41
select * from dim.groups where group_id = 41

--2017-11-21 11:53:05.353	41	NULL	Одержимые футболом | Лига Чемпионов	NULL	552999
--2017-11-21 12:05:49.760	119762339	NULL	Blue Music	NULL	203


with get_first_group_params_rn as
(
select *
  , rn = row_number() over (partition by group_id order by action_dt desc)
from fact.groups_info
)
, get_first_group_params as
(
select
    group_id 
  , name = new_name
  , members_count = new_members_count
  --, action_dt, new_members_count, new_name
from get_first_group_params_rn
where rn = 1
)
select *
from 
  get_first_group_params as by_state
  full outer join dim.groups as g
    on by_state.group_id = g.group_id
    and by_state.name = g.name
    and by_state.members_count = g.members_count
order by iif(g.group_id is null or by_state.group_id is null, 0, 1)

select count(*) from dim.groups


*/


select * from ext.groups


;with full_users_at_time_point_rn as
(
select 
    g.action_dt
  , g.group_id
  , g.[user_id]
  , g.action_type
  , rn = row_number() over (partition by g.group_id, g.[user_id] order by g.action_dt desc)
  , e.cnt_basic_users
  , e.cnt_users
  , e.k
from 
  fact.groups_growth as g
  join ext.groups as e
    on g.group_id = e.group_id
    and e.run_time = '2017-11-29 14:22:29.323'
where action_dt < '2017-11-30'
)
, full_users_at_time_point as
(
select 
    group_id
  , [user_id]
  , cnt_basic_users
  , cnt_users
  , k
  , rn = row_number() over (partition by [user_id] order by k desc)
from full_users_at_time_point_rn
where rn = 1
  and action_type != 'exit'
)
select 
    [user_id]
  , created_dt_precise = '2017-11-30'
  , created_by = 'e1_extend_users'
  , cnt_groups = count(*)
  , best_group_id = max(iif(rn = 1, group_id, null))
  , best_cnt_basic_users = max(iif(rn = 1, cnt_basic_users, null))
  , best_cnt_users = max(iif(rn = 1, cnt_users, null))
  , best_k = max(iif(rn = 1, k, null))
  , sum_k = sum(k)
  , group_extend_run_time = '2017-11-29 14:22:29.323'
from full_users_at_time_point
group by [user_id]

--8768309
--(4840743 row(s) affected)

select count(*) from dim.groups_members where group_id in 
(select group_id from ext.groups where run_time = '2017-11-29 14:22:29.323')

--- exec ext.e1_extend_users 

select * from ext.users order by sum_k desc

exec sp_updatestats