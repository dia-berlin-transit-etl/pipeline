/*
The query retrieves the identifier and coordinates of a station given its name.
The station name is provided as an input parameter.
*/
---DEALLOCATE get_station_coordinates;

PREPARE get_station_coordinates(text) AS
SELECT station_eva,
       lon,
       lat
FROM dw.dim_station
WHERE station_name= $1;

EXECUTE get_station_coordinates('Tiergarten');
EXECUTE get_station_coordinates('Alexanderplatz');