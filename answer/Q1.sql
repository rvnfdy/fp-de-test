CREATE TABLE fp-de-test.default.five_nearest_ports_to_jurong_island_port as (
WITH jurong_island_port_info as (SELECT *
FROM `bigquery-public-data.geo_international_ports.world_port_index` 
WHERE country = 'SG' AND port_name = 'JURONG ISLAND'
LIMIT 1 -- safe handling in case of data duplication
)

SELECT port_name, distance_in_meters
FROM (SELECT port_name, ST_DISTANCE(ST_GEOGPOINT(port_longitude, port_latitude), ST_GEOGPOINT((select port_longitude from jurong_island_port_info), (select port_latitude from jurong_island_port_info))) as distance_in_meters
FROM `bigquery-public-data.geo_international_ports.world_port_index`)
WHERE distance_in_meters > 0 -- to exclude the port used as reference
ORDER BY 2
LIMIT 5)