import requests
import pandas as pd
import io
import zipfile
import re

def download_csv(urls):
    """Download the latest version CSV files from nested ZIP archives after filtering out specific directories, and return as a list of pandas DataFrames."""
    all_data = {}

    for url in urls:
        print(f"Downloading {url}...")
        response = requests.get(url)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as the_zip:
                for file_name in the_zip.namelist():
                    if "_MACOSX" in file_name or not file_name.endswith('.csv'):
                        continue  # Skip non-CSV and unwanted system files

                    # Ignore empty or placeholder files based on a quick check of the file size if possible
                    if the_zip.getinfo(file_name).file_size < 100:
                        continue  # Assumes files smaller than 100 bytes are unlikely to contain useful data

                    # Extract the base identifier and version, considering only the filename, ignoring the path
                    match = re.match(r".*/(\d{6}-citibike-tripdata)(_?\d*)\.csv", file_name)
                    if match:
                        base_id = match.group(1)
                        with the_zip.open(file_name) as file:
                            try:
                                temp_df = pd.read_csv(file)
                                if temp_df.shape[0] > 0 and temp_df.shape[1] > 1:
                                    if base_id in all_data:
                                        all_data[base_id] = pd.concat([all_data[base_id], temp_df], ignore_index=True)
                                    else:
                                        all_data[base_id] = temp_df
                                else:
                                    continue  # Skip empty data frames
                            except pd.errors.EmptyDataError:
                                continue  # Skip files with no data
        else:
            response.raise_for_status()
            print(f"Failed to download {url}")

    # Drop duplicates for each DataFrame in all_data
    for key in all_data:
        all_data[key] = all_data[key].drop_duplicates()

    return list(all_data.values())

csv_urls = []

csv_data_list = download_csv(csv_urls)
print(f"Downloaded {len(csv_data_list)} CSV files.")

# Print the shape of each DataFrame to confirm data is loaded
for i, csv_data in enumerate(csv_data_list, start=1):
    print(f"CSV {i} shape: {csv_data.shape}")
