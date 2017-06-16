# --- !Ups

DROP FUNCTION IF EXISTS try_expand();
DROP TRIGGER IF EXISTS change_history_to_expand_trigger ON change_history_to_expand;
DROP FUNCTION IF EXISTS notify_change_history_to_expand();

DROP FUNCTION IF EXISTS try_compact();
DROP TRIGGER IF EXISTS change_history_to_compact_trigger ON change_history_to_compact;
DROP TABLE IF EXISTS change_history_to_compact;
DROP FUNCTION IF EXISTS notify_change_history_to_compact();

DROP TRIGGER IF EXISTS change_history_trigger ON change_history;
DROP FUNCTION IF EXISTS notify_change_history();

DROP TRIGGER IF EXISTS resource_update_trigger ON resource;
CREATE TRIGGER resource_update_trigger
AFTER UPDATE ON resource
FOR EACH ROW
WHEN (OLD.deleted <> NEW.deleted OR OLD.resource->'attributes' IS DISTINCT FROM NEW.resource->'attributes' OR OLD.resource->'relationships' IS DISTINCT FROM NEW.resource->'relationships')
EXECUTE PROCEDURE propagate_resource_edit();

# --- !Downs

DROP TRIGGER IF EXISTS resource_update_trigger ON resource;
CREATE TRIGGER resource_update_trigger
AFTER UPDATE ON resource
FOR EACH ROW
WHEN (OLD.deleted <> NEW.deleted OR OLD.resource <> NEW.resource)
EXECUTE PROCEDURE propagate_resource_edit();

CREATE FUNCTION notify_change_history() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('change_history', (SELECT MAX(seq) FROM change_history)::text);;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER change_history_trigger
AFTER INSERT OR UPDATE ON change_history
EXECUTE PROCEDURE notify_change_history();

CREATE FUNCTION notify_change_history_to_compact() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('change_history_to_compact', '');;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TABLE change_history_to_compact (
  key BIGSERIAL PRIMARY KEY,
  type TEXT NOT NULL,
  id TEXT NOT NULL
);

CREATE TRIGGER change_history_to_compact_trigger
AFTER INSERT ON change_history_to_compact
EXECUTE PROCEDURE notify_change_history_to_compact();

CREATE OR REPLACE FUNCTION try_compact() RETURNS integer AS $$
DECLARE
  compact_count integer := 0;;
BEGIN
  WITH deleted AS (DELETE FROM change_history_to_compact RETURNING type, id)
  INSERT INTO change_history (type, id)
    SELECT DISTINCT type, id FROM deleted
    ORDER BY type, id
  ON CONFLICT (type, id) DO UPDATE SET seq = GREATEST(change_history.seq, EXCLUDED.seq);;

  GET DIAGNOSTICS compact_count = ROW_COUNT;;

  RETURN compact_count;;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION notify_change_history_to_expand() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('change_history_to_expand', '');;
  RETURN null;;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER change_history_to_expand_trigger
AFTER INSERT ON change_history_to_expand
EXECUTE PROCEDURE notify_change_history_to_expand();

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
