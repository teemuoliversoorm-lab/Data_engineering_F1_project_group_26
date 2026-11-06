# Project 2: Data warehouse implementation and ETL pipelines

## Introduction

In Project 1, you designed the data architecture and dimensional model. In this project, you will implement and automate your solution. You will build a working pipeline that ingests, transforms and loads data into an analytical model.

The goal is to practise data processing, orchestration and transformation. Use Airflow to orchestrate the pipeline and dbt to build and test the dimensional model. Choose a database engine you are comfortable with, using a setup that supports these requirements.

## Requirements

### 1. Business continuity

Reuse or refine the business brief and data model from Project 1.

- Update the brief if necessary, including objectives, KPIs or business questions.
- Adjust the data architecture to reflect the tools you actually use.
- You may use the same datasets or replace a dataset if another fits your goals better.

### 2. Implementation requirements

Your pipeline must demonstrate the following.

#### 2.1. Data ingestion (Airflow)

- Use at least two distinct data sources, such as CSV files, Parquet files, APIs or database collections.
- Automate ingestion using Airflow DAGs.
- Include basic parameterisation, such as a date or file version.
- Include at least one data-quality check in the pipeline, such as a check for nulls, duplicates or values outside an expected range.
- Make the pipeline idempotent: running it again for the same input or date should not create duplicates or fail because the data has already been loaded.

#### 2.2. Data storage (your choice of database engine)

Choose a database engine you are comfortable using, such as PostgreSQL, DuckDB, ClickHouse or another suitable database. A local database is sufficient. Explain your choice in the project documentation.

Justify how the engine fits your analytical workload. If you use DuckDB or ClickHouse, highlight the relevant differences from PostgreSQL and explain why they matter for your project. Consider query patterns, data volume, update patterns and deployment needs. A short, reasoned comparison is sufficient.

Your setup must support dbt transformations and the dimensional model. You may also use MongoDB or another engine as a source or intermediate store. If your preferred engine cannot run the required dbt models, use a dbt-compatible database for the analytical layer.

- Load the ingested data into your chosen storage system.
- Use medallion architecture to separate the processing stages:
  - **Bronze:** raw ingested data, loaded through Airflow tasks using Python or another suitable ingestion tool.
  - **Silver (optional):** cleaned, standardised or deduplicated data, managed by dbt.
  - **Gold:** the star schema from Project 1, created and managed by dbt.
- Keep the layers distinct in your chosen storage setup; separate schemas, tables or naming conventions are sufficient.
- Design the physical tables to support analytical queries and incremental updates. Preserve the grain, facts and dimensions of your proposed model.

#### 2.3. Transformation (dbt)

Use dbt to transform data from the bronze layer into the gold layer, optionally using a silver layer for intermediate processing.

- In the gold layer, implement at least:
  - One fact table with a clearly stated grain.
  - Three dimension tables.
- Include at least one dbt test, such as a uniqueness or not-null test.
- Include at least one dbt YAML documentation file describing the models and their fields.
- Express the dependencies between models within dbt.

#### 2.4. Orchestration (Airflow and dbt)

- Integrate dbt transformations into the Airflow pipeline.
- The workflow must refresh the analytical model in the gold layer.
- You may also run dbt tests as part of the workflow.
- Each DAG should run from start to finish.

#### 2.5. Optional: extra-credit approaches

You may earn up to **two extra points in total** through one or more of the following approaches. Credit depends on appropriate use in a working implementation, relevance to your business questions and a clear explanation of the differences, benefits and trade-offs compared with a PostgreSQL-based relational approach. Include supporting code, queries and results in the repository.

A short, reasoned comparison is sufficient; a separate PostgreSQL implementation or performance benchmark is not required.

##### A. Columnar database engine

Use a columnar analytical engine, such as DuckDB or ClickHouse, in your implemented pipeline. Explain which relevant features you use and how they suit your analytical queries and workload compared with PostgreSQL. Connect the explanation to your project's query patterns, data volume, update patterns or deployment needs.

##### B. Semi-structured data integration

Integrate a semi-structured source, such as documents from MongoDB or JSON records from an API. This may be one of your two required sources, and your analytical database may still be PostgreSQL.

Show how the records are extracted, how their structure is handled and how they are transformed into the dimensional model. Explain how this data-handling approach compares with representing the same data in conventional PostgreSQL tables.

##### C. Neo4j serving layer

You may add Neo4j as an additional serving layer populated by your pipeline. Use it to answer a business question for which relationships, paths or network structure are useful.

Explain how the relevant entities and relationships from your relational model are represented as nodes and relationships in Neo4j.

Include at least one graph query and its result answering a business question.

Compare the graph representation and query with a PostgreSQL relational representation of the same data. Explain what changes, what the graph adds and the trade-offs of maintaining this additional layer.

The required analytical layer remains the dimensional star schema built with dbt and orchestrated through Airflow.

## Deliverables

### GitHub repository link

Include the complete project code:

- Airflow DAGs.
- The dbt project.
- SQL scripts and configuration files.

Include a `README.md` with:

- Setup and run instructions, including dependencies and environment variables.
- Screenshots or diagrams of the Airflow DAGs and dbt model dependencies.
- Results for the example analytical SQL queries from Project 1, adapted to your implemented star schema where necessary.

### Report.pdf (max 5 pages, excluding references)

Use 11-point body text. Figures, tables and screenshots count towards the five-page limit; the reference list is excluded.

- Describe any changes compared with Project 1.
- Explain the architecture and key design choices, including your database-engine justification.
- You may select representative pipelines and transformations to explain in the report. Refer to additional diagrams, code, tests and results in the repository using clear links or file paths. The repository must contain the complete runnable implementation for all required parts.
- If you claim extra credit, briefly explain the selected approach, its appropriate use and its differences from a PostgreSQL-based relational approach. Link to the supporting implementation and results in the repository.
- Include each group member's role and contribution percentage.
- Include an LLM disclosure with links to any AI/LLM chats used for this project.

## Grading

The base database-design marks assess the suitability of the chosen engine and the quality of the database design. All allowed engines, including PostgreSQL, are eligible for the full 15 base points.

Extra credit rewards the appropriate implementation and explained value of the approaches in Section 2.5, including their comparison with PostgreSQL. The shared maximum is +2 points, whether you implement one, two or all three approaches.

| Criterion | Points |
|---|---|
| Pipeline design and Airflow DAGs | 4 |
| Database design and medallion layers | 3 |
| dbt transformations and tests | 4 |
| Architecture and documentation | 2 |
| Analytical queries and results | 2 |
| **Total** | **15** |
| Optional columnar engine use, semi-structured integration or Neo4j serving layer | +2 bonus |

## Timeline

| Date and time | Event |
|---|---|
| 2026-10-19 | Project 2 published |
| 2026-10-23 23:59 | Deadline to request amendments arising from differing interpretations of the instructions |
| 2026-11-19 23:59 | Project 2 submission deadline |

## Other guidance

- Keep code clean and readable, with clear names and useful comments.
- Use configuration files or environment variables for paths and credentials rather than hard-coding them.
- Ask general questions relevant to everyone in the Moodle Q&A forum.
- For questions specific to your group, contact the teaching team through Moodle.
- See Moodle for submission details and course announcements.
