# Azure Fintech Risk Analytics Platform

This project demonstrates an end-to-end Azure Data Engineering solution
for near real-time financial transaction risk analytics.

## Architecture
SQLite → Incremental Files → ADLS Gen2 → Databricks (Bronze/Silver/Gold) → Power BI

## Key Features
- Incremental ingestion
- Bronze–Silver–Gold architecture
- Rule-based risk indicators
- Batch + Streaming design