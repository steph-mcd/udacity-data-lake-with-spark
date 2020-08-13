# Project 4: Datalakes with Spark

## Purpose
The purpose of this project is to help Sparkify move their data wharehouse to a datalake. To do this we will be building an ETL pipeline that will transform their data from the json logs stored in s3 into a data lake hosted in s3 where the data will be stored in parquet format. This will allow Sparkify to optimise their data anlytics process as they continue to increase in size and the amounts of data they store.

##Schema
The schema is made up of 4 dimension tables and 1 fact table. The 5 dimension tables are as below:
1. users - contains information relating to the users in the app
2. songs - contains information relating to the songs in Sparkify's music database
3. artists - contains information relating to the artists in Sparkify's music database
4. time - contains timestamps of records in songplays whcih has then been broken down into specific units of time to create ease for Sparkify's anlaytics team

The 1 fact table is songplays which contains records in event data that are associated with songplays. 

The choice of the above schema has been made to suit the needs adn requriements of Sparkify's analytical teams. 

## ETL Pipeline
This solution provided involves one spark job that will pick up the two types of json files stored in s3 (log data and song data) and transform the information in this to the above mentioned tables and store the results in parquet format in s3. 
The job starts by first processing the song files and using that data to create the sings and artists tables in the datalake. Then the job processes the log files data to create the users and time tables and finally it uses both song files and log files to create the songplays table in teh datalake. These tables are all stored in parquet format insidean s3 bucket.

##Project Structure
etl.py - The ETL which reads data from S3 and then processes that data with Spark and writes the results to s3 as parquet
dl.cfg - Configuration file that contains that contains AWS credentials

### Running the Script
Fill in details in the dl.cfg file for access to appropriate AW account. Then run the following command in the terminal:
```python etl.py```