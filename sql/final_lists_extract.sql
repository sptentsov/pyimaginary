------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------выгрузка по участникам похожих групп---------------------
------------------------------------------------------------------------------------------------------------------

;with src as
(
select
    tile = ntile(5) over(order by sum_k desc)
  , rnd = abs(checksum(newid())) % 10000
  , u.user_id 
  , sum_k
from 
  vk.ext.users as u
  left join dim.groups_members as m
    on u.user_id = m.user_id
    and m.group_id = 144657300
where m.user_id is null
  and u.created_dt_precise = '2017-11-29 16:22:44.240'
)
insert into fact.charged
select top 400000 --5 tiles with 80k users
    dt = cast(getdate() as date)
  , user_id
  , list_name = 
        cast(cast(getdate() as date) as varchar(max)) 
      + '_similar_groups_tile_' 
      + cast(tile as varchar(max))
      + iif(tile = 1, '_' + cast(ntile(3) over (partition by tile order by sum_k desc) as varchar(max)), '')
from src
order by rnd


--delete from  fact.charged
select cast(cast(getdate() as date) as varchar(max))



select f.user_id
  , u.sum_k
  , *
from 
  fact.charged as f
  left join ext.users as u
    on f.user_id = u.user_id
where f.list_name like '2017-11-29_similar_groups_tile_1%'
order by u.sum_k desc

select distinct 
  'select user_id from fact.charged where list_name = ''' + list_name  + ''''
from fact.charged
order by 'select user_id from fact.charged where list_name = ''' + list_name  + ''''

select user_id from fact.charged where list_name = '2017-11-29_similar_groups_tile_1_1'
select user_id from fact.charged where list_name = '2017-11-29_similar_groups_tile_1_2'
select user_id from fact.charged where list_name = '2017-11-29_similar_groups_tile_1_3'
select user_id from fact.charged where list_name = '2017-11-29_similar_groups_tile_2'
select user_id from fact.charged where list_name = '2017-11-29_similar_groups_tile_3'
select user_id from fact.charged where list_name = '2017-11-29_similar_groups_tile_4'
select user_id from fact.charged where list_name = '2017-11-29_similar_groups_tile_5'


------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------выгрузка по вступающим в похожие группы------------------
------------------------------------------------------------------------------------------------------------------
--7 декабря
-- select distinct run_time from [VK].[ext].[groups]

;with extended_groups as
(
select [group_id], k
from [VK].[ext].[groups]
where run_time = '2017-12-07 17:08:06.130' --upd dt
)
, get_last_user_action as
(
select g.*
  , e.k
  , rn = row_number() over (partition by g.group_id, g.user_id order by g.action_dt desc)
from 
  fact.groups_growth as g
  join extended_groups as e
    on g.group_id = e.group_id
where g.action_dt > '2017-11-30' --29го был последний масс апдейт схожих групп, 7 декабря следующий. вступившие за неделю.
  and g.action_dt < '2017-12-07 19:30'
  and g.action_type != 'init' --если группа только добавилась, то мы не можем понять недавно вошедших. 
)
--select
--  *, count(*) over (partition by group_id)
--from get_last_user_action
--where rn = 1
--  and action_type = 'enter' --ну нам же не нужны недавно вышедшие
--order by k desc, count(*) over (partition by group_id) desc, group_id
, group_by_user as
(
select 
    user_id
  , sum_k = sum(k)
  , tile = ntile(5) over(order by sum(k) desc)
from get_last_user_action
where rn = 1
  and action_type = 'enter' --ну нам же не нужны недавно вышедшие
group by user_id
)
insert into fact.charged
select 
    dt = cast(getdate() as date)
  , u.user_id
  , list_name = 
        cast(cast(getdate() as date) as varchar(max)) 
      + '_entered_to_similar_groups_tile_' 
      + cast(tile as varchar(max))
from 
  group_by_user as u
  left join dim.groups_members as m
    on u.user_id = m.user_id
    and m.group_id = 144657300
  left join fact.charged as c
    on u.user_id = c.user_id
where m.user_id is null
  and c.user_id is null


select * from vk.fact.groups_growth
where user_id = 99833



select distinct 
  'select user_id from fact.charged where list_name = ''' + list_name  + ''''
from fact.charged
order by 'select user_id from fact.charged where list_name = ''' + list_name  + ''''

select user_id from fact.charged where list_name = '2017-12-07_entered_to_similar_groups_tile_1'
select user_id from fact.charged where list_name = '2017-12-07_entered_to_similar_groups_tile_2'
select user_id from fact.charged where list_name = '2017-12-07_entered_to_similar_groups_tile_3'
select user_id from fact.charged where list_name = '2017-12-07_entered_to_similar_groups_tile_4'
select user_id from fact.charged where list_name = '2017-12-07_entered_to_similar_groups_tile_5'

select distinct 
  'bcp "select user_id from vk.fact.charged where list_name = ''' 
  + list_name  + '''" queryout C:\Users\admin\Documents\travel_proj\the_imagimary_one\pyimaginary\data\charged\' 
  + list_name + '.txt -c -T -S.\SQLEXPRESS'
from fact.charged
order by 
  'bcp "select user_id from vk.fact.charged where list_name = ''' 
  + list_name  + '''" queryout C:\Users\admin\Documents\travel_proj\the_imagimary_one\pyimaginary\data\charged\' 
  + list_name + '.txt -c -T -S.\SQLEXPRESS'