PREPARE get_closest_station(double precision, double precision) AS
SELECT station_name FROM dw.dim_station
ORDER BY (power(lat-$1,2) + power(lon-$2,2))
LIMIT 1;

EXECUTE get_closest_station(52.514061,12.336392);
