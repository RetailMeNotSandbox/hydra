# --- !Ups

-- edits to a resource enqueue an entry here
CREATE TABLE change_history_to_expand (
  key BIGSERIAL PRIMARY KEY,
  type TEXT NOT NULL,
  id TEXT NOT NULL
);

-- try_expand() grabs from to_expand, does the "find incoming" search, and enqueues the results here
CREATE TABLE change_history_to_compact (
  key BIGSERIAL PRIMARY KEY,
  type TEXT NOT NULL,
  id TEXT NOT NULL
);

-- try_compact() grabs from to_compact and upserts into here
CREATE TABLE change_history (
  type TEXT NOT NULL,
  id TEXT NOT NULL,
  seq BIGSERIAL NOT NULL,
  PRIMARY KEY (type, id)
);

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

-- capture initial history
INSERT INTO change_history (type, id)
  SELECT id->>'type', id->>'id' FROM resource
  ON CONFLICT (type, id) DO NOTHING;
CREATE INDEX change_history_lookup ON change_history (seq ASC, type);


-- expander functionality
CREATE OR REPLACE FUNCTION try_expand() RETURNS integer AS $$
DECLARE
  scratch change_history_to_expand%ROWTYPE;;
BEGIN
  SELECT * INTO scratch FROM change_history_to_expand ORDER BY key ASC LIMIT 1 FOR UPDATE SKIP LOCKED;;

  IF scratch.type IS NULL THEN
    RETURN 0;;
  END IF;;

  DELETE FROM change_history_to_expand WHERE key = scratch.key;;

  WITH RECURSIVE incoming(id) AS (
    VALUES (jsonb_build_object('type', scratch.type, 'id', scratch.id))
    UNION
    SELECT rr.from_id FROM resource_references rr INNER JOIN incoming ON rr.to_id = incoming.id
  )
  INSERT INTO change_history_to_compact (type, id)
  SELECT id->>'type', id->>'id' FROM incoming;;

  RETURN 1;;
END
$$ LANGUAGE plpgsql;;

CREATE FUNCTION notify_change_history_to_expand() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('change_history_to_expand', '');;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER change_history_to_expand_trigger
AFTER INSERT ON change_history_to_expand
EXECUTE PROCEDURE notify_change_history_to_expand();

-- compactor functionality
CREATE OR REPLACE FUNCTION try_compact() RETURNS integer AS $$
DECLARE
  compact_count integer := 0;;
  scratch RECORD;;
BEGIN
  CREATE TEMPORARY TABLE locked_scratch ON COMMIT DROP AS
    SELECT * FROM change_history_to_compact ORDER BY key ASC LIMIT 100 FOR UPDATE SKIP LOCKED;;

  GET DIAGNOSTICS compact_count = ROW_COUNT;;

  IF compact_count = 0 THEN
    RETURN 0;;
  END IF;;

  DELETE FROM change_history_to_compact WHERE key IN (SELECT key FROM locked_scratch);;

  FOR scratch IN (SELECT DISTINCT type, id FROM locked_scratch ORDER BY type, id) LOOP

    INSERT INTO change_history (type, id) VALUES (scratch.type, scratch.id)
    ON CONFLICT (type, id) DO UPDATE SET seq = GREATEST(change_history.seq, EXCLUDED.seq);;

  END LOOP;;

  RETURN compact_count;;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION notify_change_history_to_compact() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('change_history_to_compact', '');;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER change_history_to_compact_trigger
AFTER INSERT ON change_history_to_compact
EXECUTE PROCEDURE notify_change_history_to_compact();


-- changefeed functionality
CREATE TABLE changefeed (
  id TEXT PRIMARY KEY,
  parent_id TEXT REFERENCES changefeed (id),
  type_filter TEXT,
  max_ack BIGINT NOT NULL DEFAULT 0,
  created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_ack TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE FUNCTION notify_change_history() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('change_history', (SELECT MAX(seq) FROM change_history)::text);;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER change_history_trigger
AFTER INSERT OR UPDATE ON change_history
EXECUTE PROCEDURE notify_change_history();

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


-- this is index is unused now that we have the join table for the "find incoming" queries
DROP INDEX resource_relations;


# --- !Downs

CREATE INDEX CONCURRENTLY resource_relations ON resource USING GIN(refs);

DROP TRIGGER IF EXISTS changefeed_delete_trigger ON changefeed;
DROP FUNCTION IF EXISTS notify_changefeed_delete();
DROP TRIGGER IF EXISTS changefeed_ack_trigger ON changefeed;
DROP FUNCTION IF EXISTS notify_changefeed_ack();
DROP TRIGGER IF EXISTS change_history_trigger ON change_history;
DROP FUNCTION IF EXISTS notify_change_history();
DROP TABLE IF EXISTS changefeed;

DROP TRIGGER IF EXISTS change_history_to_compact_trigger ON change_history_to_compact;
DROP FUNCTION IF EXISTS notify_change_history_to_compact();
DROP FUNCTION IF EXISTS try_compact();

DROP TRIGGER IF EXISTS change_history_to_expand_trigger ON change_history_to_expand;
DROP FUNCTION IF EXISTS notify_change_history_to_expand();
DROP FUNCTION IF EXISTS try_expand();

CREATE OR REPLACE FUNCTION propagate_resource_edit() RETURNS trigger AS $$
BEGIN
  PERFORM sync_resource_ref(NEW.id);;

  RETURN null;;
END
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS change_history;
DROP TABLE IF EXISTS change_history_to_compact;
DROP TABLE IF EXISTS change_history_to_expand;
