# Batch Ingestion of Car Rental data from Google Storage Bucket to Snowflake table and orchestrating the job through airflow
 ***

## Short Summary of Steps Involved

* Created a storage integration refering to the specific GCP bucket prefix in snowflake db and gave necessary IAM roles to the service account in the GCS Bucket snowflake will be using.
* Created an external stage using this storage integration.
* Wrote an airlfow dag which will be picking up files from GCS bucket and upserting data into dimension table(SCD-2) and also will trigger a spark job to ingest rentals data into a fact table.
* Wrote a spark application which will be ingesting data into snowflake table and will be pushed to dataproc cluster by above-mentioned airflow workflow.

### Tools involved
*GCS,Dataproc,Spark,Snowflake,Composer(Airflow)*
