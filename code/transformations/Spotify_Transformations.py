import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_csv():
    try:
        logging.info('Reading CSV file')
        df_spotify = pd.read_csv('../../raw_data/spotify_raw.csv')
        logging.info('CSV file read')
        return df_spotify
    except Exception as e:
        logging.error(f'Error reading CSV file: {str(e)}')


def transform_csv(df_spotify):
     try:
        logging.info('Transforming CSV data')
        df_spotify.drop_duplicates(inplace=True)
        logging.info('Duplicates removed, checking for missing values')
        df_spotify = df_spotify.dropna()
        logging.info('Missing values removed, transforming text columns to title case')
        text_columns = ['artists', 'album_name', 'track_name', 'track_genre']
        for col in text_columns:
            df_spotify[col] = df_spotify[col].str.title()
        logging.info('Text columns transformed to title case, handling missing values and data types')
        df_spotify.fillna('Unknown', inplace=True)  # Placeholder example
        logging.info('Missing values handled, transforming explicit column to boolean type')
        df_spotify['explicit'] = df_spotify['explicit'].astype(bool)
        logging.info('Explicit column transformed to boolean type, transforming numerical columns to numeric type')
        numerical_columns = ['popularity', 'duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']
        for col in numerical_columns:
            df_spotify[col] = pd.to_numeric(df_spotify[col], errors='coerce')  # Coerce any errors to NaN
        logging.info('Numerical columns transformed to numeric type, dropping any NaN values introduced by conversion errors')
        df_spotify.dropna(inplace=True)
     except Exception as e:
        logging.error(f'Error transforming CSV data: {str(e)}')

def save_csv(df_spotify):
    try:
        logging.info('Saving transformed data to CSV')
        df_spotify.to_csv('../../processed_data/spotify_processed.csv', index=False)
        logging.info('Data saved to CSV')
    except Exception as e:
        logging.error(f'Error saving data to CSV: {str(e)}')