# --- !Ups

CREATE TABLE resource (
  id JSONB PRIMARY KEY,
  deleted BOOLEAN NOT NULL,
  resource JSONB NOT NULL,
  sequence_number BIGINT,
  created TIMESTAMP WITH TIME ZONE NOT NULL,
  updated TIMESTAMP WITH TIME ZONE NOT NULL,
  out_refs JSONB[] NOT NULL
);

CREATE TABLE resource_references (
  from_id JSONB NOT NULL,
  to_id JSONB NOT NULL, -- can't reference resource for to_id, since hydra allows references to resources that don't exist
  PRIMARY KEY (from_id, to_id)
);
CREATE INDEX reference_incoming_lookup ON resource_references (to_id, from_id);


CREATE OR REPLACE FUNCTION propagate_resource_edit() RETURNS trigger AS $$
BEGIN
  INSERT INTO resource_references (from_id, to_id)
    SELECT NEW.id, ref.ref
    FROM unnest(NEW.out_refs) as ref
  ON CONFLICT (from_id, to_id) DO NOTHING;;

  DELETE FROM resource_references rr
  WHERE from_id = NEW.id AND rr.to_id != ALL(NEW.out_refs);;

  INSERT INTO change_history_to_expand (type, id) VALUES (NEW.id->>'type', NEW.id->>'id');;

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
WHEN (OLD.deleted <> NEW.deleted OR OLD.resource->'attributes' IS DISTINCT FROM NEW.resource->'attributes' OR OLD.resource->'relationships' IS DISTINCT FROM NEW.resource->'relationships')
EXECUTE PROCEDURE propagate_resource_edit();

-- edits to a resource enqueue an entry here
CREATE TABLE change_history_to_expand (
  key BIGSERIAL PRIMARY KEY,
  type TEXT NOT NULL,
  id TEXT NOT NULL
);

-- ScratchExpander grabs from to_expand, expands into incoming ids, and upserts all of them into here
CREATE TABLE change_history (
  type TEXT NOT NULL,
  id TEXT NOT NULL,
  seq BIGSERIAL NOT NULL,
  PRIMARY KEY (type, id)
);

-- changefeed functionality
CREATE TABLE changefeed (
  id TEXT PRIMARY KEY,
  parent_id TEXT REFERENCES changefeed (id),
  type_filter TEXT[],
  max_ack BIGINT NOT NULL DEFAULT 0,
  created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_ack TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE FUNCTION notify_changefeed_ack() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('changefeed_' || NEW.id, NEW.max_ack::text);;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER changefeed_ack_trigger
AFTER UPDATE ON changefeed
FOR EACH ROW
WHEN (OLD.max_ack < NEW.max_ack)
EXECUTE PROCEDURE notify_changefeed_ack();

CREATE FUNCTION notify_changefeed_delete() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('changefeed_' || OLD.id, 'deleted');;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER changefeed_delete_trigger
AFTER DELETE ON changefeed
FOR EACH ROW
EXECUTE PROCEDURE notify_changefeed_delete();


# --- !Downs

DROP TRIGGER IF EXISTS changefeed_delete_trigger ON changefeed;
DROP FUNCTION IF EXISTS notify_changefeed_delete();
DROP TRIGGER IF EXISTS changefeed_ack_trigger ON changefeed;
DROP FUNCTION IF EXISTS notify_changefeed_ack();
DROP TABLE IF EXISTS changefeed;

DROP TABLE IF EXISTS change_history;
DROP TABLE IF EXISTS change_history_to_expand;

DROP TRIGGER IF EXISTS resource_update_trigger ON resource;
DROP TRIGGER IF EXISTS resource_insert_trigger ON resource;
DROP FUNCTION IF EXISTS propagate_resource_edit();
DROP TABLE IF EXISTS resource_references;
DROP TABLE IF EXISTS resource;
