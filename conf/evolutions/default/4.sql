# --- !Ups

ALTER TABLE change_history
ALTER COLUMN event_time SET DATA TYPE TIMESTAMP WITH TIME ZONE
  USING event_time AT TIME ZONE 'UTC';


# --- !Downs

ALTER TABLE change_history
ALTER COLUMN event_time SET DATA TYPE TIMESTAMP;
