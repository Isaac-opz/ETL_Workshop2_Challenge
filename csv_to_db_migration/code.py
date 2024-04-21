import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 

# Rutas a los archivos CSV
spotify_csv_path = 'raw_data/spotify_raw.csv'
grammy_csv_path = 'raw_data/grammy_raw.csv'

# Leer los datos
spotify_data = pd.read_csv(spotify_csv_path)
grammy_data = pd.read_csv(grammy_csv_path)

print(spotify_data.head())
print(grammy_data.head())

#processing
# Renombrar columnas si es necesario
spotify_data.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

# Eliminar columnas no necesarias o que no se crearán en la DB
spotify_data.drop(columns=['id'], inplace=True)

# Tratar valores nulos
spotify_data.fillna("Desconocido", inplace=True)
grammy_data.fillna("Desconocido", inplace=True)

# Cargar las variables de entorno desde .env
load_dotenv()

# Crear la conexión a la base de datos
db_connection_url = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_connection_url)

# Cargar los datos a PostgreSQL
spotify_data.to_sql('spotify_tracks', engine, if_exists='replace', index=False)
grammy_data.to_sql('grammy_awards', engine, if_exists='replace', index=False)
