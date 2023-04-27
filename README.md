# Moroccan_Real_Estate_Pipline
Moroccan Housing Pipline using Beautifulsoup4 , Airflow , s3 and PostgreSQL

<img width="4896" alt="Surfline App Architecture" src="[https://user-images.githubusercontent.com/5299312/169568492-4ada773b-a77b-485e-9c2e-b4538017ef59.png](https://github.com/Moadlaghyati/Moroccan_Real_Estate_Pipline/blob/main/pipeline%20(2).jpg?raw=true)">


## **Overview**

The pipeline collects every hour data from the Moroccan Real Estate Website https://mubawab.ma using Beautifulsoup4 for web scraping and exports a csv file to S3. Then the most recent file in S3 is downloaded to be ingested into the Postgres datawarehouse. Airflow is used for orchestration and hosted locally with docker-compose . Postgres is also running locally in a docker container.

## **ETL** 

![image](https://user-images.githubusercontent.com/125677177/234984964-cc752cd8-09a1-4959-9e9a-76a4723814b9.JPG)

## **Data Warehouse - Postgres**

![image](https://user-images.githubusercontent.com/125677177/234984773-8e8d0cc0-6dcc-4c47-bae7-3a46aa9360ce.JPG)



## **Learning Resources**

Airflow Basics:

[Airflow DAG: Coding your first DAG for Beginners](https://www.youtube.com/watch?v=IH1-0hwFZRQ)

[Running Airflow 2.0 with Docker in 5 mins](https://www.youtube.com/watch?v=aTaytcxy2Ck)

S3 Basics:

[Setting Up Airflow Tasks To Connect Postgres And S3](https://www.youtube.com/watch?v=30VDVVSNLcc)

[How to Upload files to AWS S3 using Python and Boto3](https://www.youtube.com/watch?v=G68oSgFotZA)

[Download files from S3](https://www.stackvidhya.com/download-files-from-s3-using-boto3/)

Docker Basics:

[Docker Tutorial for Beginners](https://www.youtube.com/watch?v=3c-iBn73dDE)

[Docker and PostgreSQL](https://www.youtube.com/watch?v=aHbE3pTyG-Q)

[Build your first pipeline DAG | Apache airflow for beginners](https://www.youtube.com/watch?v=28UI_Usxbqo)

[Run Airflow 2.0 via Docker | Minimal Setup | Apache airflow for beginners](https://www.youtube.com/watch?v=TkvX1L__g3s&t=389s)

[Docker Network Bridge](https://docs.docker.com/network/bridge/)

[Docker Curriculum](https://docker-curriculum.com/)

[Docker Compose - Airflow](https://medium.com/@rajat.mca.du.2015/airflow-and-mysql-with-docker-containers-80ed9c2bd340)
