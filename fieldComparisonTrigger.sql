create function find_difference_between_track_hive()
  returns TABLE(ek text)
language plpgsql
as $$
BEGIN
  RETURN QUERY SELECT event_track.ek_name as ek
from (
  (
    select
      distinct RIGHT(LEFT(CAST((json_array_elements(rawjsonschema) :: JSON -> 'propName') AS TEXT),-1),-1) as ek_name
    from pointpool
  ) event_track
  left join
  (
    select mongo_fields_name as ek_name
    from mongo_hive_fields
  ) mongo_fields
  on event_track.ek_name = mongo_fields.ek_name
)
where mongo_fields.ek_name is null;
end;
$$;


CREATE OR REPLACE FUNCTION find_difference_between_track_hive()
 RETURNS TABLE(ek text)
AS
$function$
BEGIN
  RETURN QUERY SELECT event_track.ek_name as ek
from (
  (
    select
      distinct RIGHT(LEFT(CAST((json_array_elements(rawjsonschema) :: JSON -> 'propName') AS TEXT),-1),-1) as ek_name
    from pointpool
  ) event_track
  left join
  (
    select mongo_fields_name as ek_name
    from mongo_hive_fields
  ) mongo_fields
  on event_track.ek_name = mongo_fields.ek_name
)
where mongo_fields.ek_name is null;
end;
$function$
LANGUAGE plpgsql;

drop function find_difference_between_track_hive()
select find_difference_between_track_hive();
