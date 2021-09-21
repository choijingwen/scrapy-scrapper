# scrapy-scrapper
A mini python web scrapping and pipelining project orchestrated with Airflow, containerized with Docker

## Setup
Before setting up and running Apache Airflow with docker, please install Docker and Docker Compose:
- https://docs.docker.com/get-docker/
- https://docs.docker.com/compose/install/

To create some files/folders before starting the docker container:
```sh
mkdir -p ./data ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

On all operating systems, run database migrations and create the first user account for initialization:
```sh
docker-compose up airflow-init
```

To start all services for the docker container:
```sh
docker-compose up
```

Airflow Login
- webserver url: http://localhost:8080/ 
- username: airflow
- password: airflow
- DAG id: web_scrapper (Status: Active)

Postgresql Login
- host: localhost
- port: 5432
- username: airflow
- password: airflow

Postgresql Tables for web_scrapper DAG
- schema: airflow
- table (raw_table): For loading raw scrapped data
- table (fact_table): For loading transformed fact data from raw_table
- table (dimension_table): For loading transformed dimension data from raw table

## Folder Structure
```bash
├───dags                        ## Folder for DAGs definition file
│   ├───sql                     ## Folder for any sql ETL scripts
│   ├─── ...
│   └───web_scrapper.py         ## DAG definition file for web_scrapper DAG
├───data                        ## Container mounted folder for output data from task execution                  
├───logs                        ## Logs from task execution and scheduler
│   ├───dag_processor_manager
│   ├───scheduler
│   ├─── ...
│   └───web_scrapper            ## Logs from web_scrapper DAG
└───plugins                     ## Folder for airflow custom-plugins
```