# ETL_Workshop2_Challenge

Exploring data integration and visualization through Apache Airflow. This repository showcases my efforts in creating an ETL pipeline, leveraging different data sources to produce insightful visualizations.

## Context

This repository presents my approach to the **2nd ETL Workshop**, aimed at offering hands-on experience with data pipeline creation using Apache Airflow. The project involves extracting data from diverse sources (API, CSV file, and database), performing transformations, and merging the data for further analysis and visualization. The ultimate goal is to store the processed data in a database and on Google Drive as a CSV file, and then utilize the database to generate a dashboard for data visualization.

### Data Sources:

- The **Spotify dataset** (details)
- The **Grammy Awards dataset** (details)

These datasets are combined and analyzed to create a comprehensive dashboard that showcases:

- Data integration techniques,
- Transformation processes, and
- Advanced visualizations.

Throughout this project, I utilized `Python`, `Jupyter Notebook`, `PostgreSQL` for the databases, and `Apache Airflow` for orchestrating the ETL pipeline. Visualization tools such as Looker, PowerBI, or Tableau were employed to craft the final dashboard, providing a holistic view of the music industry's trends and achievements.

### Dashboard

The dashboard visualizes the merged data from the three sources, offering insights into the music industry's dynamics, with a focus on Spotify tracks and Grammy Awards.

### Install

Basic requirements include an IDE (or any text editor) and Python3. Further dependencies and setup instructions are provided in the project documentation.

### Workshop Flow

The ETL process is visualized in Apache Airflow, highlighting the sequence of tasks from data extraction to loading, transformation, and finally visualization in the dashboard.

