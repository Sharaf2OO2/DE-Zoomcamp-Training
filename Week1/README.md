# Running PostgreSQL with Docker
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v c:/Users/DELL/Desktop/DE-Zoomcamp-Training/Week1/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```
# Running pgAdmin
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```
# Connecting Postgres and pgAdmin together
Create a network
```
docker network create pg-network
```
Run Postgres (with the created network)
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v c:/Users/DELL/Desktop/DE-Zoomcamp-Training/Week1/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```
Run pgAdmin (with the created network)
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```
# Data Ingestion
Build the image
```
docker build -t taxi_ingest:v1.0
```
Run the image
```
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

docker run -it \
  --network=pg-network \
  taxi_ingest:v1.0 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```
# Docker-Compose
Run it 
```
docker-compose up
```
Run in detached mode
```
docker-compose up -d
```
# Parquet files
To upload and work with Parquet files in Jupyter Notebook, you need to install "pyarrow" and "fastparquet" libraries
```
pip install pyarrow fastparquet
```
