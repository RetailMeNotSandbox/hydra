# --- !Ups

CREATE TABLE resource (
  id JSONB PRIMARY KEY,
  deleted BOOLEAN NOT NULL,
  refs JSONB NOT NULL,
  resource JSONB NOT NULL,
  sequence_number BIGINT,
  created TIMESTAMP WITH TIME ZONE NOT NULL,
  updated TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX resource_relations ON resource USING GIN(refs);

# --- !Downs

DROP TABLE IF EXISTS resource;
