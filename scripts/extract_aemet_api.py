"""
Extraction Script: AEMET API
This script downloads climate data in batches and saves it to the data/raw folder.
"""
import requests
import pandas as pd
import time
import os

# 1. SECURITY: Read the API Key from the environment, NEVER hardcoded.
api_key = os.environ.get("AEMET_API_KEY")

if not api_key:
    print("❌ ERROR: API Key not found.")
    print("Make sure you have created the .env file with AEMET_API_KEY=your_key")
    exit(1)

headers = {'api_key': api_key}

# 2. Dynamic paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw')

# Ensure the data/raw folder exists
if not os.path.exists(RAW_DATA_PATH):
    os.makedirs(RAW_DATA_PATH)

periods = [
    ("2025-01-01T00:00:00UTC", "2025-06-30T23:59:59UTC"),
    ("2025-07-01T00:00:00UTC", "2025-12-31T23:59:59UTC")
]

dfs_list = []
print("🚀 Starting batch download from AEMET...")

for start, end in periods:
    url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/estacion/C629X"
    print(f"\n📡 Requesting data from {start[:10]} to {end[:10]}...")
    
    response = requests.get(url, headers=headers)
    api_result = response.json()
    
    if response.status_code == 200 and api_result.get('estado') == 200:
        if 'datos' in api_result:
            print("✅ Permission granted. Downloading data chunk...")
            final_res = requests.get(api_result['datos'])
            chunk_df = pd.DataFrame(final_res.json())
            dfs_list.append(chunk_df)
        else:
            print("⚠️ 'datos' key missing in response.", api_result)
    else:
        print("❌ Error in this chunk:", api_result)
    
    time.sleep(2) # Courtesy pause to avoid rate limits

if len(dfs_list) > 0:
    annual_weather_df = pd.concat(dfs_list, ignore_index=True)
    # Save directly to the correct folder
    output_path = os.path.join(RAW_DATA_PATH, 'weather_data_2025.csv')
    annual_weather_df.to_csv(output_path, index=False)
    print(f"\n💾 SUCCESS! File saved at {output_path} with {len(annual_weather_df)} recorded days.")