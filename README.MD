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
