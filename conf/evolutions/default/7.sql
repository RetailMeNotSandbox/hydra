# --- !Ups

ALTER TABLE resource
  DROP COLUMN refs;


# --- !Downs

ALTER TABLE resource
  ADD COLUMN refs JSONB;

