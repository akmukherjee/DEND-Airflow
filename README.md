# DEND-Airflow
An ETL Pipeline created using Apache Airflow for the Udacity Data Engineering Nanodegree. The ETL Pipeline consists of developing an ETL job
to read the files from an S3 bucket and load to an AWS Redshift Cluster in Staging Tables. 

Subsequently the data is loaded on to the analysis tables
from the staging tables. A Data Quality Check is performed to verify if the data is properly loaded on the Analysis tables.
