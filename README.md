# Real-Time ETL Pipeline with CDC

A real-time data pipeline that captures MongoDB changes using CDC, processes them through Kafka and Apache Beam, and stores transformed data in PostgreSQL.

## Architecture
```
MongoDB (Orders + Customers)
    ↓ Change Streams
CDC Consumer
    ↓ Publishes Events
Kafka (orders-cdc topic)
    ↓ Consumes Events
Apache Beam Pipeline
    ├─ Parse Messages
    ├─ Enrich with Customer Data
    └─ Transform & Validate
    ↓ Writes
PostgreSQL Warehouse
```

## Tech Stack

- **MongoDB** - Source database with Change Streams
- **Apache Kafka** - Message queue
- **Apache Beam** - Data processing pipeline
- **PostgreSQL** - Data warehouse
- **Docker Compose** - Infrastructure

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+

### Setup
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start infrastructure
docker-compose up -d

# 3. Initialize MongoDB replica set
docker exec -it etl-mongodb mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"

# 4. Create warehouse
python -m src.data_warehouse.setup_warehouse

# 5. Seed data
python -m src.data_generator.seeder
```

### Run Pipeline

Open 3 terminals:
```bash
# Terminal 1: CDC Consumer
python -m src.cdc_consumer.consumer

# Terminal 2: Data Generator
python -m src.data_generator.generator

# Terminal 3: Beam Pipeline
python -m src.beam_pipeline.pipeline
```
Important note: the pipeline will shutdown after processing 20 orders, to modify that number it is recommended to edit the max_num_records variable in the pipeline.py

## Project Structure
```
src/
├── beam_pipeline/
│   └── pipeline.py           # Apache Beam ETL pipeline
├── cdc_consumer/
│   └── consumer.py           # MongoDB CDC consumer
├── common/
│   └── config.py             # Configuration
├── data_generator/
│   ├── generator.py          # Live data simulation
│   ├── seeder.py             # Initial data
│   └── models.py             # Data models
└── data_warehouse/
    ├── schema.sql            # PostgreSQL schema
    ├── setup_warehouse.py    # Warehouse setup

docker-compose.yml            # Infrastructure
requirements.txt              # Dependencies
.env                          # Configuration
```

## Configuration (.env)
```env
MONGODB_URI=mongodb://localhost:27018/?replicaSet=rs0&directConnection=true
MONGODB_DATABASE=etl_source
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=warehouse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

## Potential bottlenecks

- In case of traffic increasing by 10x one of the first things to break would be the MongoDB Change Streams since its single-threaded per collection which would make it hard for the CDC consumer to keep up.

## Rationale for each component and trade offs
- For the CDC consumer MongoDB Change Streams were chosen beacuse of the simplicity of use and the fact it requires no external libraries and tools, downside being the fact it is hard to scale unlike the alternatives for CDC like Debezium which is more suited for larger scales
- For the Warehouse PostgreSQL was chosen again because of its ease of use without needing to spend more time to set everything up and being very easy to test and run locally of course this has the 
- For the pipeline Apache Beam was chosen because it was a requirement and it is unavoidable to not use in real life scenarios where there is a large influx of data, the downside is a steeper learning curve and complexity of the setup instead of just handling everything using plain python but the upside being its very robust and can be scaled
- Kafka was chosen as it is very widely used and one of the most prominent message queues in the industry, although it might be overkill taking into account the ability to handle large ammounts of data of the components such as the CDC consumer, it is worth is as it has very powerful featurse such as replay of messages and the ability to fine tune

## Author

**Emanuel** - Backend Developer Challenge Submission
