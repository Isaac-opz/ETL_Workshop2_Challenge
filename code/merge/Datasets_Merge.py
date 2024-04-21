import re
import pandas as pd

# Load the datasets
grammy_df = pd.read_csv('data/grammy_dataset_cleaned.csv') 
spotify_df = pd.read_csv('data/spotify_dataset_cleaned.csv')

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
grammy_df[['artist', 'artist_cleaned']].head(), spotify_df[['artists', 'artists_cleaned']].head()

# Merge datasets on cleaned artist names
merged_data = pd.merge(grammy_df, spotify_df, left_on='artist_cleaned', right_on='artists_cleaned', how='inner')

# Show the number of entries in the merged dataset and preview the data
merged_data_info = merged_data.info()
merged_data.head()

# Save the merged dataset to a new CSV file
merged_data.to_csv('data/merged_data.csv', index=False)