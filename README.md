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

## Technologies Used

- **Python**: For all data transformation scripts.
- **PostgreSQL Serverless on Neon**: Database to store the transformed and merged datasets.
- **Looker Studio**: To visualize the transformed and merged data.
- **PyDrive**: Used to upload the transformed data to Google Drive as a csv file.


### Dashboard

The dashboard visualizes the merged data from two sources, offering insights into the music industry's dynamics, with a focus on Spotify tracks and Grammy Awards.

### Installation

Basic requirements include an IDE (or any text editor) and Python3.

In order to run the code and connect to the Database you'll need a `dotenv` file with the following credential, you also can work with the data by yourself using the [raw data](/raw_data)

``` DB_USERNAME='
DB_PASSWORD=''
DB_HOST=''
DB_PORT=
DB_NAME=''
```

Now that you have the `.env` file you need to set up your environment with the following command:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Now you can run all the code in the `code/transformations` folder to get the data ready for the ETL pipeline and to store your data in the databases/Google Drive.

You can also take a look at the `code/EDAs` folder to see further analysis of the data.

## ETL Pipeline

The ETL pipeline consists of the following steps:

### Data Extraction

The data extraction process involves collecting data from three sources: Spotify API, CSV file, and PostgreSQL database. The data is extracted using Python scripts and stored in a Pandas DataFrame for further processing.

### Data Transformation

The data transformation process involves cleaning, merging, and aggregating the data from the different sources. This step is crucial for preparing the data for analysis and visualization, ensuring that it is consistent and accurate.

### Data Loading

The transformed data is loaded into a PostgreSQL database and stored as a CSV file on Google Drive. This step enables easy access to the data for further analysis and visualization.

### Data Visualization

The final step involves creating a dashboard to visualize the data and provide insights into the music industry's trends and achievements. The dashboard showcases the merged data from the three sources, offering a comprehensive view of the industry's dynamics.

Here is a preview of the dashboard:

![Dashboard](/data-README.md/dashboard.png)

Also here is a preview of the charts we used to analyze the data:

### Spotify Charts

![Spotify Data](/data-README.md/spotify1.png)

![Spotify Data](/data-README.md/spotify2.png)

![Spotify Data](/data-README.md/spotify3.png)

![Spotify Data](/data-README.md/spotify4.png)

![Spotify Data](/data-README.md/spotify5.png)

![Spotify Data](/data-README.md/spotify6.png)

![Spotify Data](/data-README.md/spotify7.png)

### Grammy Charts

![Grammy Data](/data-README.md/grammy1.png)

![Grammy Data](/data-README.md/grammy2.png)
