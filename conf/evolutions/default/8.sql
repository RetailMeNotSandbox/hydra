# --- !Ups

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

# --- !Downs

CREATE OR REPLACE FUNCTION try_compact() RETURNS integer AS $$
DECLARE
  compact_count integer := 0;;
  scratch RECORD;;
BEGIN

  PERFORM pg_advisory_lock(123);;

  CREATE TEMPORARY TABLE locked_scratch ON COMMIT DROP AS
    SELECT * FROM change_history_to_compact ORDER BY key ASC LIMIT 10000 FOR UPDATE SKIP LOCKED;;

  GET DIAGNOSTICS compact_count = ROW_COUNT;;

  PERFORM pg_advisory_unlock(123);;

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


