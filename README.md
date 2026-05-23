# ShopFlow CDC Lakehouse

ShopFlow is an end-to-end real-time CDC analytics project. It streams changes from PostgreSQL through Debezium and Kafka, writes them into Apache Iceberg tables with Spark Structured Streaming, queries the lakehouse with Trino, and serves dashboards through both a React/FastAPI app and Apache Superset.

## Architecture

```text
PostgreSQL
  -> Debezium Kafka Connect
  -> Kafka topics
  -> Spark bronze stream
  -> Iceberg bronze.cdc_events on MinIO
  -> Spark silver/gold streams
  -> Iceberg silver and gold tables
  -> Trino
  -> FastAPI -> React dashboard
  -> Superset BI dashboard
```

## Tech Stack

- PostgreSQL with logical replication
- Debezium Kafka Connect
- Kafka and Zookeeper
- Spark 3.5 Structured Streaming
- Apache Iceberg
- MinIO S3-compatible object storage
- Trino
- FastAPI
- React, TypeScript, Vite
- Apache Superset

## Project Structure

```text
backend/      FastAPI API used by the dashboard
data/olist/   Olist CSV dataset files
db/           Postgres schema, loader, and realtime simulator
debezium/     Debezium connector configuration
frontend/     React dashboard
spark/        Bronze, silver, and gold Spark streaming jobs
superset/     Superset config, Trino registration, and ShopFlow color palette
trino/        Trino Iceberg catalog and memory configuration
```

## Prerequisites

- Docker Desktop
- Python virtual environment dependencies installed
- Olist CSV files under `data/olist/`

The project expects a `.env` file at the repository root. Example values:

```env
POSTGRES_DB=shopflow
POSTGRES_USER=shopflow
POSTGRES_PASSWORD=shopflow

MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123

TRINO_USER=admin

SUPERSET_SECRET_KEY=shopflow-dev-superset-secret-change-me
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
SUPERSET_ADMIN_EMAIL=admin@shopflow.local
SUPERSET_TRINO_URI=trino://admin@trino:8080/iceberg
```

## Run the Project

Start from the project root:

```powershell
cd C:\Users\pc\Desktop\BIG_DATA_Project
```

### 1. Start the Docker stack

```powershell
docker compose up -d --build
```

Check that the services are running:

```powershell
docker ps
```

### 2. Load the initial Olist data

```powershell
venv\Scripts\python.exe db\load_olist.py
```

### 3. Register the Debezium connector

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile .\debezium\connector-config.json
```

If the connector already exists, delete and recreate it:

```powershell
Invoke-RestMethod -Method Delete -Uri http://localhost:8083/connectors/shopflow-postgres-connector
```

Then run the registration command again.

Check connector status:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/shopflow-postgres-connector/status
```

### 4. Start Spark streaming jobs

Open four separate terminals.

Bronze stream:

```powershell
docker exec -it shopflow_spark /opt/spark/bin/spark-submit /opt/shopflow/spark/bronze_all_tables.py
```

Silver stream:

```powershell
docker exec -it shopflow_spark /opt/spark/bin/spark-submit /opt/shopflow/spark/silver_orders.py
```

Gold stream:

```powershell
docker exec -it shopflow_spark /opt/spark/bin/spark-submit /opt/shopflow/spark/gold_orders.py
```

Business gold metrics stream:

```powershell
docker exec -it shopflow_spark /opt/spark/bin/spark-submit /opt/shopflow/spark/gold_business_metrics.py
```

Keep these terminals running. These are streaming jobs, so they do not exit on their own.

### 5. Open the dashboard

```text
http://localhost:5173
```

Useful service URLs:

```text
Frontend:    http://localhost:5173
Backend API: http://localhost:8001
Superset:    http://localhost:8088
Kafka UI:    http://localhost:8090
MinIO:       http://localhost:9001
Trino:       http://localhost:8080
Spark UI:    http://localhost:4040
```

MinIO login:

```text
username: minio
password: minio123
```

Superset login:

```text
username: admin
password: admin
```

Superset automatically registers a Trino database named `ShopFlow Iceberg`. Use schema `gold` for dashboard-ready tables,
or `bronze` and `silver` when you want to inspect CDC internals. The Superset config also registers chart color palettes
that match the React dashboard:

```text
Categorical palette: ShopFlow React Palette
Sequential palette:  ShopFlow Rose Scale
```

Create or refresh the prebuilt PDF dashboard:

```powershell
docker exec shopflow_superset python /app/pythonpath/create_shopflow_dashboard.py
```

Create or refresh the saved SQL Lab queries:

```powershell
docker exec shopflow_superset python /app/pythonpath/create_shopflow_saved_queries.py
```

Open it at:

```text
http://localhost:8088/superset/dashboard/shopflow-cdc-lakehouse/
```

## Run Real-Time CDC Simulation

After the Docker stack, Debezium connector, and Spark streams are running, start the simulator:

```powershell
venv\Scripts\python.exe db\simulate_realtime_events.py --interval 2
```

This continuously inserts and updates simulated orders, payments, and reviews in PostgreSQL. Debezium captures these changes and pushes them through Kafka into the lakehouse.

Run a limited number of events:

```powershell
venv\Scripts\python.exe db\simulate_realtime_events.py --interval 1 --events 50
```

Stop the simulator with:

```text
Ctrl + C
```

Spark uses 30-second triggers, so dashboard updates can take 30-60 seconds to appear.

The simulator produces insert, update, delete, review, and return/cancellation events. Simulated customers include a
`customer_segment`, simulated orders include a `sales_channel`, and the `returns` table captures return or cancellation
reason/status events required by the project brief.

## Analytical Layers

- Bronze: raw CDC messages from Kafka in `iceberg.bronze.cdc_events`.
- Silver: cleaned order CDC in `iceberg.silver.orders`, plus invalid order records in `iceberg.dlq.invalid_orders`.
- Gold: analysis-ready tables including `orders_by_status`, `pipeline_latency`, `payments_by_period`,
  `returns_summary`, `customer_segments`, and `orders_by_channel`.

## Verify the Pipeline

Check Kafka topics:

```powershell
docker exec shopflow_kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Check backend health:

```powershell
Invoke-RestMethod http://localhost:8001/health
```

Check gold order status data:

```powershell
Invoke-RestMethod http://localhost:8001/api/business/orders-by-status
```

Expected response contains rows like:

```text
delivered
shipped
canceled
processing
```

Check PDF-required business gold metrics:

```powershell
Invoke-RestMethod http://localhost:8001/api/business/payments-by-period
Invoke-RestMethod http://localhost:8001/api/business/returns-summary
Invoke-RestMethod http://localhost:8001/api/business/customer-segments
Invoke-RestMethod http://localhost:8001/api/business/orders-by-channel
```

## Stop the Project

Stop only the simulator or Spark jobs with `Ctrl + C` in their terminals.

Stop containers while keeping data volumes:

```powershell
docker compose stop
```

Stop and remove containers:

```powershell
docker compose down
```

Remove containers and volumes:

```powershell
docker compose down -v
```

Use `down -v` only when you want to reset Postgres and MinIO data.

## Troubleshooting

### Frontend shows zeros

First check the backend directly:

```powershell
Invoke-RestMethod http://localhost:8001/api/business/orders-by-status
```

If the backend returns real rows, hard-refresh the browser:

```text
Ctrl + Shift + R
```

Open the dashboard at:

```text
http://localhost:5173
```

### Trino exits with code 137

Exit code `137` usually means Trino was killed because Docker ran out of memory. This repo includes a small Trino memory configuration under `trino/` to reduce memory pressure.

Check Trino status:

```powershell
docker ps -a --filter name=shopflow_trino
```

Restart Trino and backend:

```powershell
docker compose up -d trino backend
```

If it keeps crashing, increase Docker Desktop memory or stop duplicate Spark jobs.

### Do not start duplicate Spark streams

Run only one instance of each:

```text
bronze_all_tables.py
silver_orders.py
gold_orders.py
gold_business_metrics.py
```

Check Spark processes:

```powershell
docker exec shopflow_spark pgrep -af "bronze_all_tables|silver_orders|gold_orders|gold_business_metrics"
```

If you accidentally started duplicates, restart Spark:

```powershell
docker restart shopflow_spark
```

Then start one bronze, one silver, and one gold job again.

### Debezium connector already exists

Delete and recreate it:

```powershell
Invoke-RestMethod -Method Delete -Uri http://localhost:8083/connectors/shopflow-postgres-connector

Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile .\debezium\connector-config.json
```

## Notes

- Spark streaming jobs are long-running and should stay open.
- The frontend polls the backend periodically; wait a little after starting Spark jobs.
- The dashboard depends on Trino being healthy because FastAPI queries Iceberg through Trino.
- PostgreSQL changes are captured by Debezium only after the connector is registered and running.
