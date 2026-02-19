# spacenewspipeline
SpaceNews Data Engineering Pipeline
Overview
This project implements an end-to-end data engineering pipeline for ingesting, processing, and analyzing data from the Spaceflight News API. The solution follows a modern Data Lake architecture with Bronze and Silver layers, orchestrated via Apache Airflow and processed using Apache Spark.
Architecture Summary
Local Docker-based architecture includes:
- Apache Airflow (orchestration)
- PostgreSQL (metadata and ingest state)
- Spark (distributed processing)
- MinIO (S3-compatible object storage)
- Docker Compose (container orchestration)

Designed for AWS scalability:
- MWAA
- Amazon S3
- AWS Glue / EMR
- RDS
- Athena / Redshift

Pipeline Flow
1. Extract data from API (paginated and incremental)
2. Store raw JSON in Bronze layer
3. Spark transformation into Silver parquet
4. Data ready for analytics and warehouse modeling
Key Engineering Decisions
- Incremental ingestion using state table
- Idempotent DAG execution
- Partitioned storage by ingestion_date
- Structured logging
- Cloud portability design
