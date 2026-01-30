WITH st AS (
  SELECT station_eva, station_name
  FROM dw.dim_station
  WHERE station_name = :station_name  -- input name should be changed
),

latest AS (
  SELECT DISTINCT ON (fm.station_eva, fm.stop_id)
    fm.station_eva,
    fm.stop_id,
    fm.arrival_delay_min,
    fm.departure_delay_min,
    fm.arrival_cancelled,
    fm.departure_cancelled,
    fm.arrival_is_hidden,
    fm.departure_is_hidden
  FROM dw.fact_movement fm
  JOIN st ON st.station_eva = fm.station_eva
  ORDER BY fm.station_eva, fm.stop_id, fm.snapshot_key DESC, fm.movement_key DESC
),

delay_obs AS (
  SELECT arrival_delay_min AS delay_min
  FROM latest
  WHERE arrival_delay_min IS NOT NULL
    AND arrival_delay_min >= 0
    AND arrival_cancelled = FALSE
    AND arrival_is_hidden = FALSE

  UNION ALL

  SELECT departure_delay_min AS delay_min
  FROM latest
  WHERE departure_delay_min IS NOT NULL
    AND departure_delay_min >= 0
    AND departure_cancelled = FALSE
    AND departure_is_hidden = FALSE
)
SELECT
  st.station_name,
  st.station_eva,
  AVG(delay_min)::double precision AS avg_delay_min,
  COUNT(*) AS n_delay_observations
FROM delay_obs
JOIN st ON true
GROUP BY st.station_name, st.station_eva;