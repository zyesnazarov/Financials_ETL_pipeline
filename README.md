# Financial Data Engineering Pipeline

## Overview

This project was developed as part of my job at Deloitte for the valuation team to access financial data of the top 100 listed companies. The extracted data is used for valuation purposes, including benchmarking, financial ratios, and other key metrics essential for financial analysis.

This project demonstrates a financial data engineering pipeline that extracts, processes, and transforms financial data from Yahoo Finance using modern cloud and big data technologies. The pipeline automates data collection, storage, transformation, and analysis using Apache Airflow, Azure Blob Storage, Apache Spark, and Databricks.

## Usage

- The pipeline automatically scrapes and processes financial data at scheduled intervals.
- Transformed data is stored in a structured format for easy querying.
- Use Databricks and SQL to analyze financial reports
- The data is used by the valuation team in their day to day operating activity required for valuation purposes
- This solution saves a lot of time for a valuation team, since the most recent information is always available and properly structured based on the requirements.


## Architecture

Technologies Used

- **Apache Airflow**: Orchestration of data pipelines
- **BeautifulSoup**: Web scraping for financial data
- **Yahoo Finance API**: Retrieving structured financial data
- **Azure Blob Storage**: Cloud storage for raw and processed data
- **Apache Spark**: Data processing and transformation
- **Databricks**: Managed Spark environment for scalable data analysis
- **Delta Lake**: ACID-compliant data storage and versioning
- **SQL**: Querying and analyzing processed financial data

## Installation & Setup

### Prerequisites:

- Python 3.8+
- Apache Airflow
- Databricks workspace
- Azure Storage Account

### Steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/financial-data-pipeline.git
   cd financial-data-pipeline
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure Airflow DAGs:

   - Deploy `Azure_financials_pipeline_dag.py` to your Airflow environment.
   - Ensure Airflow connections to Yahoo Finance API and Azure Blob Storage are properly configured.

4. Run the Databricks Notebook:

   - Upload `Databricks_finance_etl_project.ipynb` to your Databricks workspace.
   - Configure a cluster with Apache Spark.
   - Run the notebook to transform and store financial data in Delta tables.


