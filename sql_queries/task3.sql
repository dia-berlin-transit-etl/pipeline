-- given a time snapshot (date hour)
-- return the total number of canceled trains over all 133 stations in Berlin.

-- latest as-of state per movement (station_eva, stop_id) at time S

-- Because of the Postgres-specific rule for DISTINCT ON: 
-- it keeps the first row it sees for each distinct group, according to your ORDER BY.

-- counts distinct pairs (station_eva, stop_id), so if a train is cancelled at n stations, count it n-times (once per station)


WITH params AS (
  SELECT '2509051100'::text AS s  -- snapshot cutoff S
),
latest AS (
  SELECT DISTINCT ON (fm.station_eva, fm.stop_id)
    fm.station_eva,
    fm.stop_id,
    fm.train_id,
    fm.arrival_cancelled,
    fm.departure_cancelled
  FROM dw.fact_movement fm
  JOIN params p ON true
  WHERE fm.snapshot_key <= p.s
  ORDER BY fm.station_eva, fm.stop_id, fm.snapshot_key DESC, fm.movement_key DESC
),
canceled_station_train AS (
  SELECT DISTINCT
    station_eva,
    train_id
  FROM latest
  WHERE arrival_cancelled OR departure_cancelled
)
SELECT
  COUNT(*) AS cancellations_per_station_total
FROM canceled_station_train;
