# Automated Employee Data Pipeline with Privacy Masking and Transformation

## Problem Statement
Managers and department heads need employee information, such as names, job titles, and departments, for reporting and decision-making. However, raw data contains sensitive details like salaries and passwords, which must be protected to comply with privacy regulations. A solution is needed to provide the necessary information while ensuring sensitive data remains secure and inaccessible during analysis.

## Solution Overview
This project implements an **automated ETL pipeline** that generates synthetic employee data using **Python Faker** and securely uploads it to **Google Cloud Storage**. Sensitive data, including salaries and passwords, is **masked** and **transformed** using **Google Cloud Data Fusion**, ensuring compliance with data privacy standards before it is loaded into **BigQuery** for analysis. The pipeline is orchestrated with **Cloud Composer (Apache Airflow)**, ensuring a seamless process from data generation to analysis. Finally, managers and department heads can access insights and reports through **Looker**, with sensitive information securely masked to protect employee privacy.

## Tech Stack & Architecture

![Tech Stack & Architecture](https://github.com/Kai-334/Data-Transformation-Masking-with-Data-Fusion-Airflow-and-BigQuery/blob/c287a86c3bb583c86e0407508a4344759180c7e1/Project%20Architecture.png)

This project leverages an **end-to-end data pipeline** built with the following technologies. The diagram above illustrates how the data flows across these components:

- **Python**: Used for generating synthetic employee data with the Faker library, which is then uploaded to Google Cloud Storage.
- **Google Cloud Storage**: Acts as the raw data repository for employee information.
- **Google Cloud Data Fusion**: Handles data transformation processes, including masking and encrypting sensitive employee details, ensuring compliance with data privacy standards.
- **Google BigQuery**: Stores the transformed and cleaned data, enabling efficient querying and analysis.
- **Looker**: Provides intuitive data visualisation and reporting, helping stakeholders gain insights from the employee data.
- **Cloud Composer (Apache Airflow)**: Orchestrates and automates the entire end-to-end data pipeline, ensuring smooth workflows from data generation to final reporting.

## Data Fusion Pipeline

![Data Fusion Pipeline](https://github.com/Kai-334/Data-Transformation-Masking-with-Data-Fusion-Airflow-and-BigQuery/blob/65c9645a483f3b49a2c3c80e2b13025247388fb6/Cloud%20Data%20Fusion%20Pipeline.png)

## Wrangler: Mask and Encrypt Sensitive Data 

![Wrangler: Mask and Encrypt Sensitive Data ](https://github.com/Kai-334/Data-Transformation-Masking-with-Data-Fusion-Airflow-and-BigQuery/blob/65c9645a483f3b49a2c3c80e2b13025247388fb6/Mask%20and%20Encrypt%20Sensitive%20Data%20in%20Data%20Fusion.png)

## Store Transformed Data in BigQuery

![Store Transformed Data in BigQuery](https://github.com/Kai-334/Data-Transformation-Masking-with-Data-Fusion-Airflow-and-BigQuery/blob/65c9645a483f3b49a2c3c80e2b13025247388fb6/Store%20Transformed%20Data%20in%20BigQuery.png)

## Reporting using Looker Studio

![Reporting using Looker Studio](https://github.com/Kai-334/Data-Transformation-Masking-with-Data-Fusion-Airflow-and-BigQuery/blob/65c9645a483f3b49a2c3c80e2b13025247388fb6/Looker%20for%20reporting.png)

## Automation using Composer (Airflow)

![Automation using Composer (Airflow)](https://github.com/Kai-334/Data-Transformation-Masking-with-Data-Fusion-Airflow-and-BigQuery/blob/78038a57c81f9bcf4a572dd443fefbea339bc5cd/Composer%20Airflow.png)

