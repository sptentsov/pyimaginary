USE [VK]
GO
/****** Object:  StoredProcedure [discovering].[merge_groups]    Script Date: 20.11.2017 17:43:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER proc [discovering].[merge_groups]
as

with existing_groups as
(
select distinct group_id 
from fact.groups_growth
)
, mark_first_seen_groups as
(
select 
    s.group_id
  , s.[user_id]
  , is_group_first_seen = iif(e.group_id is null, 1, 0)
from 
  staging.groups_members as s
  left join existing_groups as e
    on s.group_id = e.group_id
)
MERGE dim.groups_members AS target  
USING mark_first_seen_groups AS source 
  ON target.group_id = source.group_id
  and target.[user_id] = source.[user_id]  
WHEN NOT MATCHED by target THEN   
  INSERT (group_id, [user_id])  
  VALUES (source.group_id, source.[user_id])
WHEN NOT MATCHED by source THEN  
  delete
OUTPUT
    getdate() --action_dt
  , isnull(deleted.group_id, inserted.group_id)
  , isnull(deleted.[user_id], inserted.[user_id])
  , case
      when $action = 'DELETE' then 'exit'
      when $action = 'INSERT' and source.is_group_first_seen = 1 then 'init'
      when $action = 'INSERT' and source.is_group_first_seen = 0 then 'enter'
    end
  into fact.groups_growth
;
