import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_db():
    try:
        logging.info('Connecting to the database')
        load_dotenv()
        db_connection_url = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(db_connection_url)
        with engine.connect() as connection:
            logging.info('Executing SQL query')
            result = connection.execute("SELECT * FROM grammy_awards")
            logging.info('Query executed')
            grammy_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logging.info('Data retrieved from the database')
        return grammy_df
    except Exception as e:
        logging.error(f'Error reading data from the database: {str(e)}')

def transform_db(grammy_df):
    try:
        # Remove duplicates
        logging.info('Removing duplicates')
        grammy_df = grammy_df.drop_duplicates()
        logging.info(f"Duplicates removed, new dataset size: {grammy_df.shape}")
        # Check for missing values
        logging.info('Checking for missing values')
        missing_values = grammy_df.isnull().sum()
        logging.info(f"Missing values:\n{missing_values}")
        # Fill missing values with 'Unknown'
        logging.info('Filling missing values with "Unknown"')
        grammy_df.fillna('Unknown', inplace=True)
        # Standardize the 'artist' field
        logging.info('Standardizing the "artist" field')
        grammy_df['artist'] = grammy_df['artist'].str.title()
        grammy_df['artist'] = grammy_df['artist'].str.strip()
        logging.info('Changes made to the "artist" field')
        # Ensure 'year' is integer type and filter valid years
        logging.info('Filtering valid years')
        grammy_df['year'] = grammy_df['year'].astype(int)
        valid_years = grammy_df['year'].between(1958, 2024)
        grammy_df = grammy_df[valid_years]
        # Standardize category names to lower case and strip extra whitespace
        logging.info('Standardizing category names')
        grammy_df['category'] = grammy_df['category'].str.lower().str.strip()
        logging.info('Category names standardized')
        # Check for short or null entries in 'nominee' and 'artist'
        logging.info('Checking for short or null entries')
        grammy_df['nominee'] = grammy_df['nominee'].fillna('Unknown')
        grammy_df['artist'] = grammy_df['artist'].fillna('Unknown')
        grammy_df['nominee_length'] = grammy_df['nominee'].str.len()
        short_entries = grammy_df[grammy_df['nominee_length'] < 5]
        logging.info(f"Short or null entries:\n{short_entries}")
    except Exception as e:
        logging.error(f'Error transforming database data: {str(e)}')

def save_df_to_csv(grammy_df):
    try:
        logging.info('Saving transformed data to CSV')
        grammy_df.to_csv('../../processed_data/grammy_processed.csv', index=False)
        logging.info('Data saved to CSV')
    except Exception as e:
        logging.error(f'Error saving data to CSV: {str(e)}')


