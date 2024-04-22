import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 

spotify_csv_path = 'raw_data/spotify_raw.csv'
grammy_csv_path = 'raw_data/grammy_raw.csv'

spotify_data = pd.read_csv(spotify_csv_path)
grammy_data = pd.read_csv(grammy_csv_path)

print(spotify_data.head())
print(grammy_data.head())

spotify_data.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

spotify_data.drop(columns=['id'], inplace=True)

spotify_data.fillna("Desconocido", inplace=True)
grammy_data.fillna("Desconocido", inplace=True)

load_dotenv()

db_connection_url = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_connection_url)

spotify_data.to_sql('spotify_tracks', engine, if_exists='replace', index=False)
grammy_data.to_sql('grammy_awards', engine, if_exists='replace', index=False)
