# 🚖 Real-Time Ride Intelligence Platform

A production-style real-time Data Engineering platform built using Azure services and Databricks to process, transform, and analyze ride-event streaming data at scale.

## 📌 Project Overview

This project demonstrates the design and implementation of a modern real-time analytics architecture using:
- Azure Event Hub (Kafka)
- Azure Databricks
- Delta Live Tables (DLT)
- Delta Lake
- Azure Data Factory
- ADLS Gen2

The platform ingests ride-event streams in real time, processes them through Medallion Architecture layers (Bronze, Silver, Gold), and builds analytics-ready Star Schema models for reporting and business intelligence.

The architecture focuses on:
- scalable streaming ingestion
- reliable ETL pipelines
- CDC-based incremental processing
- fault tolerance
- late-arriving event handling
- real-time analytical readiness

## 🚀 Key Features

- ⚡ Real-time ride-event ingestion
- 🔄 Kafka-based streaming architecture using Azure Event Hub
- 🧠 Declarative ETL pipelines using Delta Live Tables (DLT)
- 🛡️ Fault-tolerant streaming workflows
- ⏱️ Watermarking for late-arriving data
- 💾 Checkpointing for reliable stream recovery
- 🏗️ Medallion Architecture implementation
- 🔁 CDC-based incremental processing
- 📊 Star Schema modeling for analytics
- ☁️ Cloud-native scalable architecture

## 🛠️ Technologies Used

- Azure Data Factory
- Azure Databricks
- PySpark
- SQL
- Azure Event Hub (Kafka)
- Delta Lake
- Delta Live Tables (DLT)
- Change Data Capture (CDC)
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Medallion Architecture
- Star Schema Modeling
