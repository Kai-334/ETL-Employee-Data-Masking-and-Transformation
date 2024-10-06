# Employee Data Pipeline with Masking and Transformation

In this project, a table contains detailed information about employees in the company, including sensitive data such as **salary** and **passwords**. To generate an employee report while protecting privacy, sensitive data must be masked and encoded before analysis. 

**Employee data** is generated using **Python Faker** and uploaded to **Google Cloud Storage** with Python. This project creates an automated ETL pipeline that masks and transforms sensitive information using **Google Cloud Data Fusion** before loading the data into **BigQuery** for analysis. The pipeline is orchestrated with **Cloud Composer** and visualized in **Looker**.

## Tech Stack

This project leverages the following technologies:

- **Python**: Used for data extraction.
- **Cloud Composer (Apache Airflow)**: Orchestrates and automates the data pipeline.
- **Google Cloud Data Fusion**: Transforms and masks sensitive data.
- **Google BigQuery**: A data warehouse used for querying and analysis.
- **Google Cloud Storage**: Stores raw and transformed data.
- **Looker**: Provides data visualization and reporting.

## Architecture

![Project Architecture](https://github.com/Kai-334/Data-Transformation-Masking-with-Data-Fusion-Airflow-and-BigQuery/blob/c287a86c3bb583c86e0407508a4344759180c7e1/Project%20Architecture.png)

### Description:
1. **Python** extracts data and saves it to **Cloud Storage**.
2. **Cloud Data Fusion** masks and transforms the data, then loads it into **BigQuery**.
3. **Looker** visualizes the data from **BigQuery**.
4. **Airflow** (via **Cloud Composer**) automates and manages the entire process.
