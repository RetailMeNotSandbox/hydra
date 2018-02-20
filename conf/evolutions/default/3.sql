# --- !Ups

ALTER TABLE change_history
ALTER COLUMN event_time SET NOT NULL;


# --- !Downs

ALTER TABLE change_history
ALTER COLUMN event_time DROP NOT NULL;
