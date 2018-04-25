# --- !Ups

CREATE INDEX change_history_lookup ON change_history (seq, type);


# --- !Downs

DROP INDEX IF EXISTS change_history_lookup;
