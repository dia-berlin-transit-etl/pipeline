SELECT station_eva, station_name, lon, lat
FROM dw.dim_station
WHERE station_name = :station_name; -- input name should be changed