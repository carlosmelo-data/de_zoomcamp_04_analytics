<<<<<<< HEAD
# de_zoomcamp_04_analytics
=======
# Queries created to solve homework.

-- Creating external table referring to gcs path

CREATE OR REPLACE EXTERNAL TABLE `my-first-project-412820.ny_taxi.external_green_tripdata_hw_dw`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-carlos-melo/green_taxi_dw_hw/13f496b879d8464da55e52101dbb1a21-0.parquet']
);

-- Check green trip data

SELECT * FROM my-first-project-412820.ny_taxi.external_green_tripdata_hw_dw limit 10;

-- Create a non partitioned table from external table

CREATE OR REPLACE TABLE my-first-project-412820.ny_taxi.green_tripdata_non_partitioned AS
SELECT * FROM my-first-project-412820.ny_taxi.external_green_tripdata_hw_dw;

-- Q1

SELECT COUNT(*) FROM my-first-project-412820.ny_taxi.external_green_tripdata_hw_dw;
SELECT COUNT(*) FROM my-first-project-412820.ny_taxi.green_tripdata_non_partitioned;

-- Q2
-- Scanning 0MB of data

SELECT COUNT(DISTINCT(PULocationID))
FROM my-first-project-412820.ny_taxi.external_green_tripdata_hw_dw;

-- Scanning 6.41 MB of DATA

SELECT DISTINCT(PULocationID)
FROM my-first-project-412820.ny_taxi.green_tripdata_non_partitioned;

-- Q3
-- 1622

SELECT COUNT(*)
FROM my-first-project-412820.ny_taxi.green_tripdata_non_partitioned
WHERE fare_amount = 0.0;

-- Q4
-- Creating a partition and cluster table

CREATE OR REPLACE TABLE my-first-project-412820.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM my-first-project-412820.ny_taxi.green_tripdata_non_partitioned;

-- Q5
-- Query scans 12.92 MB

SELECT DISTINCT(PUlocationID) as PU_Ids
FROM my-first-project-412820.ny_taxi.green_tripdata_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Query scans 1.12 MB

SELECT DISTINCT(PUlocationID) as PU_Ids
FROM my-first-project-412820.ny_taxi.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Q6

-- GCP Bucket (gs://mage-zoomcamp-carlos-melo/green_taxi_dw_hw/13f496b879d8464da55e52101dbb1a21-0.parquet)

-- G7

-- False

-- G8
-- 0B

SELECT COUNT(*) 
FROM my-first-project-412820.ny_taxi.green_tripdata_partitoned_clustered;
>>>>>>> c78d0b0 (first commit)
