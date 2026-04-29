---
title: Data Integration Platform of Heterogeneous Sources
year: 2025
tags: [Java, Spring Boot, PostgreSQL, REST API, ETL, JWT, OpenCSV]
cover: project_images/main.jpg
---

## What is it

A Spring Boot platform that ingests data from heterogeneous sources (CSV files and PostgreSQL databases), applies configurable field mappings and type transformations, and consolidates records into a unified queryable dataset.

## Problem it solves

Organizations typically store data across disconnected systems — relational databases, flat files, and exports — each with its own schema and field naming conventions. Joining this data for analysis requires writing and maintaining custom one-off scripts for every source pair. This platform replaces that manual work with a configurable ETL pipeline that connects to multiple source types, normalizes fields and data types, and automatically detects relationships between records sharing common identifiers across sources.

## How it works

- **Hardware:** Software-only project; runs on any JVM-compatible host or container.
- **Software:** Java 17, Spring Boot 3.3, Spring Security (JWT / OAuth2 Resource Server), Spring Data JPA, PostgreSQL, OpenCSV, Springdoc OpenAPI. Implements an adapter pattern for source extraction (CSV and DB adapters), a configurable field-mapping and type-casting transform layer, asynchronous ingestion runs with status tracking, and a graph-based relationship derivation engine that indexes shared identifier fields across all ingested sources.
- **Key challenge:** Automatically deriving relationships between records from different source types (CSV and DB) without explicit foreign key definitions. Solved by building a shared-identifier index across all ingested records in a single pass and using a bidirectional adjacency graph to link entities that share canonical field values (IDs, codes, or names), regardless of which source they originated from.

## Results

- Unified records from CSV files and PostgreSQL databases in a single pipeline run with field-level mapping configuration.
- Relationship graph correctly linked cross-source entities sharing common identifier fields with no manual join configuration.
- REST API secured with JWT authentication, backed by full unit and integration test coverage across controllers, services, and adapters.
