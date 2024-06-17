# Data Pipeline

## Overview
This data pipeline is designed to ingest blockchain events from Alchemy, process them using RabbitMQ for message queuing, and store them in a PostgreSQL database. The pipeline is containerized using Docker and Grafana is used for visualizing the data. It works near real time. No historical data is inserted.

## Modules

- **Listener**: Receives blockchain events from latest Alchemy and processes them with RabbitMQ.
- **Consumer**: Writes the received events from RabbitMQ into a PostgreSQL database and loads the Bundlers list.
-- **Dashboard**: Displays a chart in Grafana for User Operation events from the Entry Point contract differntiating by Biconomy bundlers

## Pipeline Diagram
![Pipeline Diagram](./diagram.png)

## Setup

1. Clone the repository
```
git clone https://github.com/cr4n/entry_point.git
cd entry_point
```

2. Set up the environment variables in an `.env` followings `env.sample` guidelines:
   - `ALCHEMY_URL`: Alchemy URL for querying blockchain data
   - `ENTRY_POINT_ADDRESS`: Ethereum address to pull events from
   - Leave the rest as it comes

3. Build and start the services:
```
docker-compose up --build
```
4. Open Grafana:
```
https://localhost:3000
```
and update the password in the connector to `postgres` (REQUIRED)

## DB Data Dump  
- Query the DB:
```
docker exec -it postgres psql -U postgres -d BICO -c "select * from pipeline.raw_user_operation;"
```
- Make a DB dump:
```
docker exec -it postgres pg_dump -U postgres -d BICO -F c -f /tmp/entry_point_db.dump
docker cp postgres:/tmp/entry_point_db.dump ./entry_point_db.dump
```
- Make a CSV dump:
```
docker exec -it postgres psql -U postgres -d BICO -c "\COPY (SELECT * FROM pipeline.raw_user_operation) TO '/tmp/raw_user_operation.csv' CSV HEADER;"
docker cp postgres:/tmp/raw_user_operation.csv ./raw_user_operation.csv
```

