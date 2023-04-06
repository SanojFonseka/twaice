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