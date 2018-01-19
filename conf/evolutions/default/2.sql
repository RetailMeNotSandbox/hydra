# --- !Ups

ALTER TABLE change_history
ADD COLUMN event_time TIMESTAMP;

# --- !Downs

ALTER TABLE change_history
DROP COLUMN IF EXISTS event_time;
