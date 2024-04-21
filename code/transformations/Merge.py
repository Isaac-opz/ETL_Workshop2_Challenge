import pandas as pd
import logging
import re
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def merge_datasets(grammy_df, spotify_df):
    grammy_df = pd.read_csv('processed_data/grammy_processed.csv') 
    spotify_df = pd.read_csv('processed_data/spotify_processed.csv')
    try:
        # Function to clean and normalize artist names
        def clean_artist_names(name):
            # Convert to lower case
            name = name.lower()
            # Remove special characters and extra spaces
            name = re.sub(r"[^\w\s]", '', name)
            name = re.sub(r"\s+", ' ', name).strip()
            return name

        # Apply cleaning function to both datasets
        grammy_df['artist_cleaned'] = grammy_df['artist'].apply(clean_artist_names)
        spotify_df['artists_cleaned'] = spotify_df['artists'].apply(clean_artist_names)

        # Display cleaned data
        logging.info("Grammy dataset after cleaning:")
        logging.info(grammy_df[['artist', 'artist_cleaned']].head())
        logging.info("Spotify dataset after cleaning:")
        logging.info(spotify_df[['artists', 'artists_cleaned']].head())

        # Merge datasets on cleaned artist names
        merged_data = pd.merge(grammy_df, spotify_df, left_on='artist_cleaned', right_on='artists_cleaned', how='inner')

        # Show the number of entries in the merged dataset and preview the data
        logging.info("Merged dataset information:")
        merged_data_info = merged_data.info()
        logging.info("Merged dataset preview:")
        logging.info(merged_data.head())

        # Save the merged dataset to a new CSV file
        merged_data.to_csv('processed_data/merged_data.csv', index=False)
    except Exception as e:
        logging.error(f'Error merging datasets: {str(e)}')


