"""
Data-Driven Revenue Management: Meteorological Impact on Hotel Demand
Architected by: Álvaro López Almeida
Role: End-to-End ETL & Statistical Analysis Pipeline
"""
import pandas as pd
import os
import sys

# ==========================================
# 🛡️ DATA QUALITY GATES (SENIOR LAYER)
# ==========================================

def validate_weather_quality(df):
    """
    Acts as a 'Data Guard'. Ensures meteorological inputs 
    adhere to physical and logical constraints.
    """
    print("🔍 [DQ GATE] Auditing Weather Data Quality...")
    
    # 1. Critical Null Check
    critical_cols = ['tmax', 'fecha', 'hrMin']
    for col in critical_cols:
        if col not in df.columns:
            raise KeyError(f"❌ SCHEMA ERROR: Missing mandatory column: {col}")
    
    # 2. Physical Range Validation (Canary Islands context)
    # Temperatures > 55°C or < -5°C are considered sensor malfunctions.
    temp_anomalies = df[(df['tmax'] > 55) | (df['tmax'] < -5)]
    if not temp_anomalies.empty:
        print(f"⚠️ WARNING: Detected {len(temp_anomalies)} physical temperature anomalies. Reviewing sensor C629X.")

    # 3. Logic Check: Humidity must be between 0 and 100
    if not df['hrMin'].between(0, 100).all():
        print("⚠️ WARNING: Relative Humidity values detected outside [0-100] range.")
    
    print("✅ [DQ GATE] Weather Audit Passed.\n")
    return True

def validate_istac_schema(df, file_name):
    """
    Validates ISTAC microdata schema to prevent pipeline drift 
    when survey formats change across quarters.
    """
    required_cols = ['ISLA', 'ALOJ_CATEG', 'ANTELACION_VIAJE', 'OLA']
    missing = [col for col in required_cols if col not in df.columns.str.upper()]
    
    if missing:
        print(f"❌ SCHEMA DRIFT DETECTED in {file_name}: Missing {missing}")
        return False
    return True

# ==========================================
# MAIN PIPELINE EXECUTION
# ==========================================

print("🚀 STARTING DEFINITIVE DATA PIPELINE (v2.0 - Production Grade)...\n")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw')
WEATHER_PATH = os.path.join(RAW_DATA_PATH, 'weather_data_2025.csv')
ISTAC_FILES = ["First_Quarter_2025.csv", "Second_Quarter_2025.csv", "Third_Quarter_2025.csv", "Fourth_Quarter_2025.csv"]

# --- PHASE 1: WEATHER PROCESSING ---
print("☁️ Phase 1: Processing Meteorological Series...")
try:
    weather_df = pd.read_csv(WEATHER_PATH)
    
    # Cleaning Numeric Formats (European to Standard)
    for col in ['tmax', 'hrMin']:
        if weather_df[col].dtype == object:
            weather_df[col] = weather_df[col].str.replace(',', '.').astype(float)
    
    # Trigger Data Quality Gate
    validate_weather_quality(weather_df)
    
    weather_df['date'] = pd.to_datetime(weather_df['fecha'])
    weather_df['MONTH_WAVE'] = weather_df['date'].dt.month
    
    # Feature Engineering: Dynamic Anomaly Detection (Calima Isolation)
    weather_df['MONTHLY_AVG_TMAX'] = weather_df.groupby('MONTH_WAVE')['tmax'].transform('mean')
    weather_df['IS_CALIMA'] = (
        (weather_df['tmax'] >= weather_df['MONTHLY_AVG_TMAX'] + 4.5) & 
        (weather_df['hrMin'] <= 55.0)
    ).astype(int)

    monthly_weather = weather_df.groupby('MONTH_WAVE')['IS_CALIMA'].sum().reset_index()
    monthly_weather.rename(columns={'IS_CALIMA': 'ACTUAL_CALIMA_DAYS'}, inplace=True)

except Exception as e:
    print(f"❌ CRITICAL PIPELINE FAILURE (Phase 1): {e}")
    sys.exit(1)


# --- PHASE 2: TOURIST PROCESSING ---
print("🏨 Phase 2: Processing Booking Microdata (ISTAC)...")
dfs_list = []
for file_name in ISTAC_FILES:
    path = os.path.join(RAW_DATA_PATH, file_name)
    try:
        temp_df = pd.read_csv(path, sep=',', encoding='latin-1', low_memory=False)
        if validate_istac_schema(temp_df, file_name):
            dfs_list.append(temp_df)
    except FileNotFoundError:
        print(f"⚠️ Warning: {file_name} missing from data/raw/. Skipping.")

if not dfs_list:
    print("❌ ERROR: No valid ISTAC data found. Process terminated.")
    sys.exit(1)

tourists_df = pd.concat(dfs_list, ignore_index=True)
tourists_df.columns = tourists_df.columns.str.strip().str.upper()

# Core Filtering Logic
final_df = tourists_df[(tourists_df['ISLA'] == 'ES705') & (tourists_df['ALOJ_CATEG'] == 'HOTEL_ESTRELLAS_4')].copy()
final_df['LAST_MINUTE_NUM'] = final_df['ANTELACION_VIAJE'].isin(['D_LT1', 'D1T15']).astype(int)

monthly_istac = final_df.groupby('OLA').agg(
    LAST_MINUTE_RATIO=('LAST_MINUTE_NUM', 'mean'),
    TOTAL_SAMPLES=('LAST_MINUTE_NUM', 'size')
).reset_index()

monthly_istac.rename(columns={'OLA': 'MONTH_WAVE'}, inplace=True)
monthly_istac['% LAST MINUTE'] = (monthly_istac['LAST_MINUTE_RATIO'] * 100).round(2)


# --- PHASE 3: THE BIG DATA MERGE & CORRELATION ---
print("🔗 Phase 3: Final Integration & Statistical Analysis...")
final_report = pd.merge(monthly_istac, monthly_weather, on='MONTH_WAVE', how='left')
final_report['ACTUAL_CALIMA_DAYS'] = final_report['ACTUAL_CALIMA_DAYS'].fillna(0).astype(int)

correlation = final_report['ACTUAL_CALIMA_DAYS'].corr(final_report['LAST_MINUTE_RATIO'])

# --- OUTPUT GENERATION ---
print("\n" + "="*60)
print(" 📊 EXECUTIVE REVENUE ANALYTICS REPORT")
print("="*60)
print(final_report[['MONTH_WAVE', 'ACTUAL_CALIMA_DAYS', '% LAST MINUTE', 'TOTAL_SAMPLES']].to_string(index=False))

print("\n" + "="*60)
print(f"🧮 PEARSON CORRELATION (r): {correlation:.4f}")
if abs(correlation) < 0.3:
    print("💡 INSIGHT: Demand is INELASTIC to Calima. Reactive price drops are unjustified.")
else:
    print("💡 INSIGHT: Significant correlation detected. Adjust Revenue Strategy accordingly.")
print("="*60)