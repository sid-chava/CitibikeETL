import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import pandas as pd
import io
import zipfile
import re

# Setup Chrome options
chrome_options = Options()
chrome_options.add_argument("--start-maximized")

PATH = "C:\\Program Files (x86)\\chromedriver.exe"
service = Service(PATH)

print("Initializing WebDriver")
driver = webdriver.Chrome(service=service, options=chrome_options)
csv_files = []

# Function to perform human-like cursor movements
def human_like_hover(driver, element, pause_time=1):
    actions = ActionChains(driver)
    actions.move_to_element(element).perform()
    time.sleep(pause_time)

print("Opening URL")
driver.get("https://s3.amazonaws.com/tripdata/index.html")

# Wait for the table body to load
try:
    print("Waiting for table body to load")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "tbody-content"))
    )
    print("Table body loaded successfully")
except Exception as e:
    print(f"Timeout waiting for page to load: {e}")
    driver.quit()
    exit()

# Verify the content of the table body
try:
    print("Finding table body element")
    tbody = driver.find_element(By.ID, "tbody-content")
    print("Table body inner HTML:")
    print(tbody.get_attribute('innerHTML'))
except Exception as e:
    print(f"Error finding elements: {e}")
    driver.quit()
    exit()

# Wait for the links to be present
try:
    WebDriverWait(driver, 20).until(
        EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
    )
    print("Links are present in the table body")
except Exception as e:
    print(f"Timeout waiting for links: {e}")
    driver.quit()
    exit()

# Find all anchor tags within the table body
try:
    print("Finding all anchor tags within the table body")
    links = tbody.find_elements(By.TAG_NAME, "a")
except Exception as e:
    print(f"Error finding elements: {e}")
    driver.quit()
    exit()

print(f"Found {len(links)} links")

# Move cursor over each link and print the href
for link in links:
    try:
        href = link.get_attribute("href")
        if href:
            # Move the cursor to the link
            print(f"Hovering over link: {href}")
            human_like_hover(driver, link, pause_time=random.uniform(0.5, 1.5))
            print(f"Link: {href}")
            if "zip" in href:
                csv_files.append(href)
        # Random delay to mimic human behavior
        time.sleep(random.uniform(0.5, 2))
    except Exception as e:
        print(f"Error processing link: {e}")

print("Closing driver")
driver.quit()

print(f"Found {len(csv_files)} urls files:")

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

    # Drop duplicates for each DataFrame in all_data
    for key in all_data:
        all_data[key] = all_data[key].drop_duplicates()

    return list(all_data.values())

# List of URLs
csv_files = [
    
]

csv_data_list = download_csv(csv_files)

# Print the shape of each DataFrame to confirm data is loaded
print("Data loaded successfully, DF count: ", len(csv_data_list))
