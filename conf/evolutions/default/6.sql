# --- !Ups

ALTER TABLE resource
ALTER COLUMN out_refs SET NOT NULL,
ALTER COLUMN refs DROP NOT NULL;

-- start accumulating events for eventual changefeed processing
CREATE OR REPLACE FUNCTION propagate_resource_edit() RETURNS trigger AS $$
BEGIN
  INSERT INTO resource_references (from_id, to_id)
    SELECT NEW.id, ref.ref
    FROM unnest(NEW.out_refs) as ref
  ON CONFLICT (from_id, to_id) DO NOTHING;;

  DELETE FROM resource_references rr
  WHERE from_id = NEW.id AND NOT rr.to_id != ALL(NEW.out_refs);;

  INSERT INTO change_history_to_expand (type, id) VALUES (NEW.id->>'type', NEW.id->>'id');;

  RETURN null;;
END
$$ LANGUAGE plpgsql;


-- we used to call this from propagate_resource_edit, but now we've just inlined it, and we've already run & deleted the job that called this
DROP FUNCTION sync_resource_ref(syncId jsonb);

# --- !Downs


CREATE OR REPLACE FUNCTION sync_resource_ref(syncId jsonb) RETURNS void AS $$
DECLARE
  syncRefs JSONB := '[]';;
BEGIN
  -- assumptions:

  -- the resource edit has a 'FOR [NO KEY] UPDATE' row lock on the resource row, this prevents another xact from hitting
  -- this trigger and trying to modify resource_references for the same resource in parallel

  -- any incoming reads for the resource will grab a 'FOR [KEY] SHARE' row lock on the resource row, which will wait
  -- until the above 'FOR UPDATE' row lock is released, preventing any resource read from ever seeing an incomplete
  -- application of INSERT/DELETEs against resource_references

  -- a noop if we're coming from the propagate edit trigger, but we do it to enforce safety
  SELECT refs INTO syncRefs FROM resource WHERE resource.id = syncId FOR UPDATE;;

  INSERT INTO resource_references (from_id, to_id)
    SELECT syncId, ref.value
    FROM jsonb_array_elements(syncRefs) as ref
  ON CONFLICT (from_id, to_id) DO NOTHING;;

  DELETE FROM resource_references rr
  WHERE from_id = syncId AND NOT syncRefs @> jsonb_build_array(rr.to_id);;

END
$$ LANGUAGE plpgsql;


-- start accumulating events for eventual changefeed processing
CREATE OR REPLACE FUNCTION propagate_resource_edit() RETURNS trigger AS $$
BEGIN
  INSERT INTO resource_references (from_id, to_id)
    SELECT NEW.id, ref.value
    FROM jsonb_array_elements(NEW.refs) as ref
  ON CONFLICT (from_id, to_id) DO NOTHING;;

  DELETE FROM resource_references rr
  WHERE from_id = NEW.id AND NOT NEW.refs @> jsonb_build_array(rr.to_id);;

  INSERT INTO change_history_to_expand (type, id) VALUES (NEW.id->>'type', NEW.id->>'id');;

  RETURN null;;
END
$$ LANGUAGE plpgsql;

ALTER TABLE resource
ALTER COLUMN out_refs DROP NOT NULL,
ALTER COLUMN refs SET NOT NULL;
