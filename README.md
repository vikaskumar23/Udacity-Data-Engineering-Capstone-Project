# Udacity-Data-Engineering-Capstone-Project

A capstone project is developed using big data tools like Spark, Redshift, ElasticSearch and airflow is used as orchestration tool to analyze trends in immigration data.
## Introduction

An ETL Procees is developed that fetch the data from Amazon S3, procees it and stores it in Amazon Redshift tables for analysis. Then data is pushed to ElasticSearch for analysis and visualizations are created in Kibana, which helps to analyze the trends in US Immigration. In this project immigration data is provided by udacity and happiness report is fetched from kaggle.

## Project Scope

The goal of this project is to analayze the trends in US Immigration and the factors affecting the trends. In this project data is prepared for anlaysis.
Following trends can be analyzed:
1. Proportion of Gender i.e. Count of Males and Females
2. Proportion of Purpose i.e. Count of People visit for Pleasure, Business, Studies
3. Immigration traffic per weekday
4. Distribution of different Airlines used by immigrants
5. Average time spent in United States by Immigrants for different purpose.
6. Number of Immigrants coming from country that has economy less than/greater than United States
7. Number of Immigrants coming from country that are more happy than united states
8. Proportion of Immigrants for each Purpose i.e. Pleasure, Business, Studies

and many more ...

## Data Required for Project

#### There are 5 datasets that need to be used for this project:
- **I94 Immigration Data:** This data comes from the US National Tourism and Trade Office.
    - Data consists of US immigration data for Jan-Dec 2016 - The April dataset will be used for this project
    - Source of Dataset : https://travel.trade.gov/research/reports/i94/historical/2016.html
    - This dataset contains all the information of US Immigrants like arrival_date, age, purpose, airline etc.

- **World Happiness Index Data:** This data is fetched from kaggle
    - Data consists from 2015-2019 - The 2016 dataset will be used for this project.
    - Source of Dataset: https://www.kaggle.com/unsdsn/world-happiness
This dataset "World Happiness Report" is a landmark survey of the state of global happiness. It contains data like happiness rank, economy, happiness score, region etc. for each country.

- **I94 Country Data:** This data is created from data dictionary provided by udacity for Immigration Data.
    - This country data is present in resources directory as "country.csv".
    - This dataset contains the country id and country name

- **I94 Visa Data:** This data is created from data dictionary provided by udacity for Immigration Data.
    - This Visa data is present in resources directory as **visa.csv**.
    - This dataset contains the visa_type id and visa purpose

- **I94 Mode Data:** This data is created from data dictionary provided by udacity for Immigration Data.
    - This mode data is present in resources directory as **mode.csv**.
    - This dataset contains the mode id and mode

## Explore and Assess the Data

### The immigration data needs to be cleaned as it has many quality issues.
**Following issues are fixed:**
1. All the rows are removed that contain null values except airline as airline can have null value if mode is different than air.
2. Field names are changed as per requirement.
3. The rows are removed if the mode is air and still the airline field is empty.
4. The rows are removed in which departure date is less than arrival_date.
5. The sas date format is converted to readable format "YYYY-MM-DD"
6. Data types for fields are changed

## Data Model

**Star Schema for this project, as this schema is easily understandable and easy to use in joins.
It has one fact table and 3 dimension tables.**
![DAG Diagram](https://github.com/vikaskumar23/Udacity-Data-Engineering-Capstone-Project/blob/master/resources/db_model.png)
#### Fact Table
- **Immigration Fact Table**
#### Dimension Table
- **Happiness Dimension Table**
- **Visa Dimension Table**
- **Mode Dimension Table**

#### Steps necessary to pipeline the data into the chosen data model:
-   First the immigration data is cleaned with the help of spark job and loaded into redshift table.
-   Country data and happiness data is loaded into Redshift country and happiness table respectively and Then country data is combined with happiness data to create happiness dimension table.
-   Visa and Mode data is loaded into visa and mode dimension table.

## ETL Process

#### Architecture
![Architecture](https://github.com/vikaskumar23/Udacity-Data-Engineering-Capstone-Project/blob/master/resources/Architecture.png)
1. Airflow triggers the clean spark job on EMR Cluster
2. Airflow triggers Redshift to create tables and load data from S3, and check for data quality issues
3. Airflow triggers spark job on EMR cluster to load data from Redshift, join them and push data to Elasticsearch. 
4. Airflow hits elasticsearch API to check data quality issues
#### ETL Job Flow:
![DAG Diagram](https://github.com/vikaskumar23/Udacity-Data-Engineering-Capstone-Project/blob/master/resources/DagImage.PNG)
1. Spark Clean Job fetch the immigration data from S3 and cleans the data for quality issues then save the cleaned data to s3.
2. Tables are created in Redshift and data is loaded from s3 to staging tables.
3. Facts and Dimension table are created and data is loaded from staging tables.
4. Then a table is created by combining all the facts and dimension tables and the resulting table is then saved to S3.
5. Then Spark Elastic job fetch data from Redshift and after some transformations data is pushed to elasticsearch.
6. At last Dashboard is created from data present in ElasticSearch to see the trends.
## Environment Required

- 2 node cluster of m5x.large of Amazon EMR : To run spark Jobs for cleaning data and pushing data to elastic
- 2 node cluster of dc2.large of Amazon Redshift : To store data in Relational Database
- t2.large 1 node EC2 instance : To run Airflow and setup ElasticSearch and Kibana

## Environment Setup

### Environment Setup EC2:
- Open inbound ports 5601, 9200, 8080 from security rules.
- Fill the config file config.ini with the public ip of EC2 on which  Elasticsearch is installed.

##### Airflow Setup-
1. To setup Airflow, this Medium Article can be referred: [Setup Airflow on Ubuntu EC2 Instance](https://medium.com/@abraham.pabbathi/airflow-on-aws-ec2-instance-with-ubuntu-aff8d3206171)
2. Install ssh operator and ssh tunnel to use ssh operator
```pip install 'apache-airflow[ssh]' sshtunnel```
3. To use Livy Operator install airflow livy library
```pip install apache-airflow-backport-providers-apache-livy[http]```
4. Move the dags and plugins  from this repo to airflow directory ```/home/airflow/airflow/``` on EC2
5. In Airflow webpage Connections create connection with AWS, Redshift and SSH for submitting spark jobs to EMR
    - **AWS:** Fill connection name as aws_credentials, connection type as 'Amazon Web Services' login and password with aws_access_key and aws_secret_access_key respectively.
    - **Redshift:** Fill Connection name as redshift, connection type as Postgres, schema as database name, user as db username, password as dbpassword, host as redshift cluster access URL and port as 5439.
    - **SSH:** Fill host as public-ip of EMR, select connection type as SSH then in extras put
        ```
        {
        "key_file": "/path/to/key/access_key.pem",
        "no_host_key_check": "true",
        "allow_host_key_change": "true"
        }
        ```
##### ElasticSearch And Kibana Setup-

Use the below commands to install elasticsearch and Kibana:
```
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-7.x.list
sudo apt-get update && sudo apt-get install elasticsearch
sudo apt-get update && sudo apt-get install kibana
```
Config Setup for ```elasticsearch.yml``` present at ```/etc/elasticsearch/```:
```
network.host : 0.0.0.0
discovery.seed_hosts : ["private-ip-of instance"]
cluster.initial_master_nodes : ["private-ip-of instance"]
```
Config Setup for ```kibana.yml``` present at ```/etc/kibana/```:
```
server.host : 0.0.0.0
```
Open Kibana at ```public-ip:5601``` then go to stack management then saved object then click on Import and move the ```dashboard.json``` from this repo source directory there.
### Redshift Cluster Setup:
Setup redhsift cluster from Amazon Redshift Console. Make sure the cluster is Publicly accessible, and redshift has proper roles and access to s3.

### EMR Cluster Setup:
Move all the spark jobs to ```/home/hadoop/``` directory.
And download the following jars there using the below commands in the ```/home/hadoop/``` directory:
```
wget "https://repo1.maven.org/maven2/com/epam/parso/2.0.10/parso-2.0.10.jar"
wget "http://dl.bintray.com/spark-packages/maven/saurfang/spark-sas7bdat/2.1.0-s_2.11/spark-sas7bdat-2.1.0-s_2.11.jar"
wget "https://oss.sonatype.org/content/repositories/snapshots/org/elasticsearch/elasticsearch-hadoop/7.8.2-SNAPSHOT/elasticsearch-hadoop-7.8.2-20200817.003026-37.jar"
wget "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.14/postgresql-42.2.14.jar"
```
## Dashboard

Snapshot of Final Dashboard
![Dashboard](https://github.com/vikaskumar23/Udacity-Data-Engineering-Capstone-Project/blob/master/resources/Image%207.jpg)
To access the live dashboard visit dashboard section of kibana accessible at ```public-ip-of-EC2:5601```

## Project Write Up

#### Rationale for the choice of tools and technologies for the project:
This project uses many tools and technologies like:
1. **Spark** : It is a big data technology to process and analyze data. It performs all tasks in parallel fashion. With this tool it is easy to handle large datasets.
2. **Redshift** : It is a columnar based MPP database. It is optimised for aggregations as well as heavy processing loads.
3. **Airflow** : It is used to orchestrate the ETL jobs. It handles all the tasks in required sequence automatically. No manual intervention is required.
4. **ElasticSearch**: It is the heart of ELK stack. It is a document based NoSql database. It is used to store data for analysis.
5. **Kibana**: It is part of ELK stack and it is used to visualize the anlaysis and to create dashboards on the them.

All the above tools used are together to handle large amount of datasets and create a reliable ETL Pipeline.

#### Other Scenarios
##### If the data was increased by 100 x
- Then also this architecture works fine, it just need to scale up infrastructure i.e. increase the cluster size of Redshift. Redshift is optimised for aggregations and heavy workloads. And for cleaning and processing we can partion data by size or date and scale up the EMR Clusters. As everything is on AWS cloud so that is easily possible to scale.
- The second approch could be that we can use NoSql databased like Cassandra, Elastic or Mongo etc.

##### If the pipelines were run on a daily basis by 7am
- As the current architecture uses airflow to orchestrate the ETL jobs. So with the help of Airflow DAG will be scheduled to run jobs daily at 7 AM.
##### If the database needed to be accessed by 100+ people
- Concurrency Scaling can be used with Redshift so that if required Redshift will automatically sacle up the clusters to meet the needs of concurrency.
loyment by navigating to your server address in your preferred browser.

## Refrences
- www.udacity.com/
- https://www.elastic.co/guide/en/elasticsearch/reference/current/deb.html
- https://docs.aws.amazon.com/
- https://airflow.apache.org/docs
- https://medium.com/@abraham.pabbathi/airflow-on-aws-ec2-instance-with-ubuntu-aff8d3206171
- https://www.kaggle.com/unsdsn/world-happiness