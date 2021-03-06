USE [VK]
GO
/****** Object:  StoredProcedure [discovering].[merge_posts]    Script Date: 12.12.2017 13:20:07 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create proc [discovering].[merge_likes_and_reposts]
as

;with processed_posts as 
(
select distinct target_id, target_post_id, is_target_a_user
from staging.likes_and_reposts
)
, mark_likes_and_reposts_as_updated as
(
select f.*
  , is_post_updated = iif(p.target_id is not null, 1, 0) 
from 
  fact.likes_and_reposts as f
  left join processed_posts as p
    on f.target_id = p.target_id
    and f.target_post_id = p.target_post_id
    and f.is_target_a_user = p.is_target_a_user
)
MERGE mark_likes_and_reposts_as_updated AS target  
USING staging.likes_and_reposts AS source 
  ON target.is_like = source.is_like
  and target.actor_id = source.actor_id
  and target.is_actor_a_user = source.is_actor_a_user
  and target.target_id = source.target_id
  and target.target_post_id = source.target_post_id
  and target.is_target_a_user = source.is_target_a_user
when matched and target.is_disabled = 1 then --если кто-то снял лойс/репост, а потом его вернул - выпиливаем дизэйбл
  update set 
      target.is_disabled = 0
    , target.disabled_dt_precise = null
when not matched by source and target.is_post_updated = 1 then
  update set
      target.is_disabled = 1
    , target.disabled_dt_precise = getdate()
when not matched by target then   
  INSERT 
    (
        is_like
      , actor_id, is_actor_a_user
      , target_id, target_post_id, is_target_a_user
      , created_dt_precise, is_disabled, disabled_dt_precise
    )
  VALUES
    (
        source.is_like
      , source.actor_id, source.is_actor_a_user
      , source.target_id, source.target_post_id, source.is_target_a_user
      , getdate() --created_dt_precise
      , 0 --is_disabled
      , null --disabled_dt_precise
    )
;

