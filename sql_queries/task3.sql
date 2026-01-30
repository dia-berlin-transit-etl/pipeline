WITH params AS (
  SELECT :snapshot::text AS s  -- input name should be changed
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