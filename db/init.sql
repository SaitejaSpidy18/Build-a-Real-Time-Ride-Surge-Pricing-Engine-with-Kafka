CREATE TABLE IF NOT EXISTS zone_stats (
  city_zone        VARCHAR PRIMARY KEY NOT NULL,
  active_drivers   INTEGER NOT NULL,
  pending_requests INTEGER NOT NULL,
  surge_multiplier FLOAT   NOT NULL,
  last_updated     TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS surge_alerts (
  alert_id         UUID PRIMARY KEY NOT NULL,
  city_zone        VARCHAR NOT NULL,
  surge_multiplier FLOAT   NOT NULL,
  timestamp        TIMESTAMP NOT NULL
);
