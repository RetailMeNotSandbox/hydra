# --- !Ups

ALTER TABLE changefeed
ALTER COLUMN type_filter
TYPE TEXT[] USING CASE WHEN type_filter IS NULL THEN NULL ELSE ARRAY[type_filter] END;

ALTER TABLE resource
ADD COLUMN out_refs JSONB[];


# --- !Downs

ALTER TABLE changefeed
ALTER COLUMN type_filter
TYPE TEXT USING CASE WHEN array_length(type_filter, 1) IS NULL THEN NULL ELSE type_filter[1] END;

ALTER TABLE resource
DROP COLUMN IF EXISTS out_refs;

