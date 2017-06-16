# --- !Ups

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


# --- !Downs

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
