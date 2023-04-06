# TWAICE data engineering test

## Instructions to run the application

### Run Locally Using Docker Container

#### Create Docker Image Using Repository

on command prompt change directory to the project file location of your local PC

run this command on prompt  to create a docker image `docker build -t twaice_sanojfonseka .`

once the docker image created

#### Run Application On Docker Container

run this command to deploy the application on a docker container `docker run twaice_sanojfonseka:latest` or `docker run <docker-image-id>`

## Approach for generating solutions

### Enrich data (Task 1)

load raw data from parquet table and metadata from the csv file

enrich sensor data by doing a join

write data to separate parquet tables by the sensor type to enusure future requirements and partitioned by date and container id

### Calculate cumulative throughtput (Task 2)
read enriched data for each sensor type

calculate cumulative throughput using pyspark window functions

write cumulative throughput data to parquet table for future requirements and partitioned by date and container id

### SQL query optimization (Task 3)

optimized SQl query is located in output directory.

first created a derived table that calculates the average m_voltage per m_container_id. Then, joined the measurements table with this derived table on the m_container_id column, and add an additional condition in the ON clause to filter only the rows where m_voltage is greater than the average m_voltage for that m_container_id. By this way there is no corelated subqueries and also prevent unnecessary comparisons between values.