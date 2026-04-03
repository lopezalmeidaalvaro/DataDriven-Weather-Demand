"""
Analytics Pipeline: Meteorological Impact on Bookings
Merges ISTAC data with AEMET climate records.
"""
import pandas as pd
import os

print("🚀 STARTING DEFINITIVE DATA PIPELINE...\n")

# Dynamic path configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw')

WEATHER_PATH = os.path.join(RAW_DATA_PATH, 'weather_data_2025.csv')
ISTAC_FILES = [
    "First_Quarter_2025.csv", 
    "Second_Quarter_2025.csv", 
    "Third_Quarter_2025.csv", 
    "Fourth_Quarter_2025.csv"
]

# ==========================================
# PHASE 1: WEATHER PROCESSING (AEMET)
# ==========================================
print("☁️ Processing weather data (Searching for Anomalies)...")
try:
    weather_df = pd.read_csv(WEATHER_PATH)
except FileNotFoundError:
    print(f"❌ ERROR: Cannot find {WEATHER_PATH}. Run extract_aemet_api.py first or place the CSV in that folder.")
    exit(1)

for col in ['tmax', 'hrMin']:
    if weather_df[col].dtype == object:
        weather_df[col] = weather_df[col].str.replace(',', '.').astype(float)

weather_df['date'] = pd.to_datetime(weather_df['fecha'])
weather_df['MONTH_WAVE'] = weather_df['date'].dt.month

weather_df['MONTHLY_AVG_TMAX'] = weather_df.groupby('MONTH_WAVE')['tmax'].transform('mean')
weather_df['IS_CALIMA'] = (
    (weather_df['tmax'] >= weather_df['MONTHLY_AVG_TMAX'] + 4.5) & 
    (weather_df['hrMin'] <= 55.0)
).astype(int)

monthly_weather = weather_df.groupby('MONTH_WAVE')['IS_CALIMA'].sum().reset_index()
monthly_weather.rename(columns={'IS_CALIMA': 'ACTUAL_CALIMA_DAYS'}, inplace=True)


# ==========================================
# PHASE 2: TOURIST PROCESSING (ISTAC)
# ==========================================
print("🏨 Processing booking microdata...")
dfs_list = []
for file_name in ISTAC_FILES:
    path = os.path.join(RAW_DATA_PATH, file_name)
    try:
        dfs_list.append(pd.read_csv(path, sep=',', encoding='latin-1', low_memory=False))
    except FileNotFoundError:
        print(f"⚠️ Warning: Could not find {file_name} in data/raw/")

if not dfs_list:
    print("❌ ERROR: No ISTAC data available to process.")
    exit(1)

tourists_df = pd.concat(dfs_list, ignore_index=True)
tourists_df.columns = tourists_df.columns.str.strip().str.upper()

final_df = tourists_df[(tourists_df['ISLA'] == 'ES705') & (tourists_df['ALOJ_CATEG'] == 'HOTEL_ESTRELLAS_4')].copy()
final_df['LAST_MINUTE_NUM'] = final_df['ANTELACION_VIAJE'].isin(['D_LT1', 'D1T15']).astype(int)

monthly_istac = final_df.groupby('OLA').agg(
    LAST_MINUTE_RATIO=('LAST_MINUTE_NUM', 'mean'),
    TOTAL_TOURISTS=('LAST_MINUTE_NUM', 'size')
).reset_index()

monthly_istac.rename(columns={'OLA': 'MONTH_WAVE'}, inplace=True)
monthly_istac['% LAST MINUTE'] = (monthly_istac['LAST_MINUTE_RATIO'] * 100).round(2)


# ==========================================
# PHASE 3: THE BIG DATA MERGE
# ==========================================
print("🔗 Merging databases...")
final_report = pd.merge(monthly_istac, monthly_weather, on='MONTH_WAVE', how='left')
final_report['ACTUAL_CALIMA_DAYS'] = final_report['ACTUAL_CALIMA_DAYS'].fillna(0).astype(int)

print("\n" + "="*50)
print(" 📊 FINAL REPORT (CLEAN DATA)")
print("="*50)
print(final_report[['MONTH_WAVE', 'ACTUAL_CALIMA_DAYS', '% LAST MINUTE', 'TOTAL_TOURISTS']].to_string(index=False))

correlation = final_report['ACTUAL_CALIMA_DAYS'].corr(final_report['LAST_MINUTE_RATIO'])

print("\n" + "="*50)
print(" 🧮 FINAL STATISTICAL ANALYSIS")
print("="*50)
print(f"Pearson Correlation Index (r): {correlation:.3f}")
print("="*50)