PREPARE get_closest_station(double precision, double precision) AS
SELECT station_eva, station_name, lon, lat
FROM dw.dim_station
ORDER BY (
  power(lat - $1, 2) +
  power((lon - $2) * cos(radians($1)), 2)
)
LIMIT 1;
