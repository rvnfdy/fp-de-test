CREATE TABLE fp-de-test.default.nearest_port_from_distress_location AS (
SELECT country, port_name, port_latitude, port_longitude
FROM (SELECT country, port_name, port_latitude, port_longitude, ST_DISTANCE(ST_GEOGPOINT(port_longitude, port_latitude), ST_GEOGPOINT(-38.706256, 32.610982)) as distance_in_meters
FROM `bigquery-public-data.geo_international_ports.world_port_index`
WHERE provisions = true
AND water = true 
AND fuel_oil = true 
AND diesel = true)
ORDER BY distance_in_meters ASC
LIMIT 1)
