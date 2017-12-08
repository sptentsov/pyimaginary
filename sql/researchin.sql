;with spammed_guys as
(
select user_id
from fact.charged
where list_name = '2017-11-29_similar_groups_tile_1_1'
)
, entered_guys as
(
select g.action_dt
  , g.user_id
from 
  fact.groups_growth as g
  join spammed_guys as s
    on g.user_id = s.user_id
where g.group_id = 144657300
  and g.action_type = 'enter'
)
select best_group_id
  , best_k
  , count(*)
  , max(best_cnt_users)
  , count(*) * 1. / max(best_cnt_users)
  , rank() over (order by best_k desc)
from 
  entered_guys as e
  left join ext.users as u
    on e.user_id = u.user_id
group by best_group_id, best_k
order by count(*) * 1. / max(best_cnt_users) desc
--order by sum_k desc

--select top 100 * from