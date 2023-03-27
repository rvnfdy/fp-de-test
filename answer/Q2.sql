CREATE TABLE fp-de-test.default.country_with_largest_number_of_ports_with_cargo_wharf as (
SELECT country, COUNT(CASE cargo_wharf WHEN TRUE THEN 1 END) as port_count
FROM `bigquery-public-data.geo_international_ports.world_port_index` 
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1)
