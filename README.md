# AzureDataEngineer_Challenge
Challenge to develop ETL pipelines, transformations and visualizations from CSV files

Challenge Instructions

![Challenge Instructions](https://github.com/user-attachments/assets/736fd5d4-eca9-471c-8cf3-b4838d26c7fc)

Data Engineering Solution for Beverage Sales Analytics Platform
ABInBev Business Case | LinkedIn Project Showcase

Project Overview
I designed and implemented a scalable Data Engineering solution for ABInBev's Beverage Sales Analytics Platform, addressing complex data integration, transformation, and aggregation challenges. The solution leveraged dual architectures (Delta Lake + Azure SQL) to ensure flexibility, performance, and compliance with Medallion architecture best practices.

Key Challenges & Solutions
1. Data Integration & Quality
Challenge: Merging disparate CSV files (beverage_sales and beverage_channel_features) with inconsistent encodings, missing values, and non-standardized fields (e.g., $ VOLUME with negative values).
Solution:

Automated Encoding Detection: Python script to detect and handle file encodings (UTF-8, ISO-8859-1).

Data Standardization:

Converted $ VOLUME to absolute values.

Removed prefixes/suffixes (e.g., TSR_PCKG_NM).

Validated schema consistency using PySpark.

2. Dimensional Modeling
Challenge: Designing a scalable star schema to support business KPIs (e.g., regional sales, brand performance).
Solution:

Fact-Dimension Model:

Fact Table: fact_sales (transactional data).

Dimensions: dim_month, dim_flavor, dim_package, dim_channel_group.

Surrogate Keys: Created for dimensions to ensure referential integrity.

3. Pipeline Orchestration
Challenge: Coordinating parallel processing across Delta Lake and Azure SQL with dependencies.
Solution:

Azure Data Factory Pipelines:

Delta Lake Path:

pipeline_transform_silver: Cleansing, deduplication, and dimension creation.

pipeline_gold_aggregation: Pre-computed KPIs (Top 3 Trade Groups, Monthly Brand Sales).

Azure SQL Path: JDBC connectors with indexed tables for OLTP queries.

Triggers: Daily execution at 11:00 AM (Brasilia time).

4. Performance Optimization
Challenge: Slow queries on large datasets.
Solution:

Delta Lake: Partitioning by region_desc and year-month.

Azure SQL: Strategic indexes on region_desc and selling_date.

Z-Ordering: Applied to Delta Tables for faster columnar scans.

5. Monitoring & Governance
Challenge: Tracking pipeline failures and data lineage.
Solution:

Logging: Azure Storage logs for pipeline runs, activity, and triggers.

Future Improvements:

Great Expectations: Automated data quality tests.

Azure Key Vault: Secure credential management.

Solution Architecture
1. Medallion Architecture
Bronze (Raw): Raw CSV ingestion to Azure Blob Storage.

Silver (Cleansed): Validated, standardized tables in Delta Lake/Azure SQL.

Gold (Aggregated): Business-ready KPIs (e.g., regional sales rankings).

2. Dual Implementation
Component	Delta Lake (Data Lake)	Azure SQL (OLTP)
Storage	Parquet files (optimized)	Indexed relational tables
Processing	PySpark for transformations	Stored procedures + SQL
Use Case	Analytical queries	Transactional reporting
3. Business Insights
Top 3 Trade Groups per Region: Ranked by sales volume.

Monthly Brand Performance: Aggregated by BRAND_NM and month_id.

Lowest-Selling Brands by Region: Identified growth opportunities.

Technologies Used
🛠 Data Processing: PySpark, SQL (Azure DB).
☁ Cloud: Azure Blob Storage, Azure Data Factory, Delta Lake.
📊 Data Modeling: Star Schema, Medallion Architecture.
⚙ Orchestration: Azure Data Factory, Triggers.

Lessons Learned & Future Enhancements
🔹 Incremental Processing: Reduce Silver layer latency with CDC (Change Data Capture).
🔹 CI/CD Pipelines: Git integration for version-controlled deployments.
🔹 Cost Optimization: Auto-scaling clusters for Delta Lake processing.

🔗 GitHub: [https://github.com/PedroBressane/AzureDataEngineer_Challenge]
📩 Let’s discuss! [Your LinkedIn Profile]

#DataEngineering #Azure #DeltaLake #ABInBev #ETL #DataArchitecture

Why This Solution?
✔ Scalable: Handles 10x data growth with partitioning.
✔ Resilient: Automated logging and alerting for pipeline failures.
✔ Business-Aligned: KPIs directly answer stakeholder questions.

"The dual-architecture approach future-proofs the platform for both analytical and transactional needs."
