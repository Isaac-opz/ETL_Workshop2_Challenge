from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import os

# Load environment variables
load_dotenv()

# Database connection URL
db_connection_url = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_connection_url)

# Load the CSV data into a DataFrame
data = pd.read_csv('data/merged_data.csv')

# Define the SQL command to create a table adapted to the dataset
sql_create_table = text("""
CREATE TABLE IF NOT EXISTS merged_data (
    year INTEGER,
    title VARCHAR(255),
    published_at TIMESTAMP,
    updated_at TIMESTAMP,
    category VARCHAR(255),
    nominee VARCHAR(255),
    artist VARCHAR(255),
    workers TEXT,
    img TEXT,
    winner BOOLEAN,
    nominee_length INTEGER,
    artist_cleaned VARCHAR(255),
    track_id VARCHAR(255),
    artists VARCHAR(255),
    album_name VARCHAR(255),
    track_name VARCHAR(255),
    popularity INTEGER,
    duration_ms INTEGER,
    explicit BOOLEAN,
    danceability FLOAT,
    energy FLOAT,
    key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INTEGER,
    track_genre VARCHAR(255),
    artists_cleaned VARCHAR(255)
);
""")

# Execute the SQL command to create the table
with engine.connect() as connection:
    connection.execute(sql_create_table)

# Insert the data into the database
data.to_sql('merged_data', engine, if_exists='replace', index=False)

