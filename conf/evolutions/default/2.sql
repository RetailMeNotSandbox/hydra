# --- !Ups

CREATE TABLE resource_references (
  from_id JSONB NOT NULL,
  to_id JSONB NOT NULL, -- can't reference resource for to_id, since hydra allows references to resources that don't exist
  PRIMARY KEY (from_id, to_id)
);
CREATE INDEX reference_incoming_lookup ON resource_references (to_id, from_id);


CREATE FUNCTION sync_resource_ref(syncId jsonb) RETURNS void AS $$
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


CREATE FUNCTION propagate_resource_edit() RETURNS trigger AS $$
BEGIN
  PERFORM sync_resource_ref(NEW.id);;

  RETURN null;;
END
$$ LANGUAGE plpgsql;


CREATE TRIGGER resource_insert_trigger
AFTER INSERT ON resource
FOR EACH ROW
EXECUTE PROCEDURE propagate_resource_edit();

CREATE TRIGGER resource_update_trigger
AFTER UPDATE ON resource
FOR EACH ROW
WHEN (OLD.deleted <> NEW.deleted OR OLD.resource <> NEW.resource)
EXECUTE PROCEDURE propagate_resource_edit();


# --- !Downs

DROP TRIGGER IF EXISTS resource_update_trigger ON resource;
DROP TRIGGER IF EXISTS resource_insert_trigger ON resource;
DROP FUNCTION IF EXISTS propagate_resource_edit();
DROP FUNCTION IF EXISTS sync_resource_ref(syncId JSONB);
DROP TABLE IF EXISTS resource_references;
