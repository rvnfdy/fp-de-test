from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

table_id = "fp-de-test.default.country_with_largest_number_of_ports_with_cargo_wharf"

job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """
SELECT country, COUNT(CASE cargo_wharf WHEN TRUE THEN 1 END) as port_count
FROM `bigquery-public-data.geo_international_ports.world_port_index` 
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))
