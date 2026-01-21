-- given a time snapshot (date hour)
-- return the total number of canceled trains over all 133 stations in Berlin.

-- latest as-of state per movement (station_eva, stop_id) at time S

-- Because of the Postgres-specific rule for DISTINCT ON: 
-- it keeps the first row it sees for each distinct group, according to your ORDER BY.

WITH params AS (
    SELECT '2509051100':: text AS s
),

latest AS (
  SELECT DISTINCT ON (fm.station_eva, fm.stop_id)
    fm.train_id,
    fm.arrival_cancelled,
    fm.departure_cancelled
  FROM dw.fact_movement fm
  JOIN params p ON true
  WHERE fm.snapshot_key <= p.s
  ORDER BY fm.station_eva, fm.stop_id, fm.snapshot_key DESC, fm.movement_key DESC
)
SELECT
  COUNT(DISTINCT train_id) AS canceled_trains -- distinct because cancellation propogates
FROM latest
WHERE arrival_cancelled OR departure_cancelled;
