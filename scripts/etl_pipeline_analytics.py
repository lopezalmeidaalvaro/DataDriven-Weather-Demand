"""
Data-Driven Revenue Management: Meteorological Impact on Hotel Demand
Architected by: Álvaro López Almeida
Role: Professional End-to-End ETL & Statistical Analysis Pipeline
Version: 2.2 (Windows & Data Quality Patch)
"""

import os
import sys
import logging
from pathlib import Path

import pandas as pd
import pandera.pandas as pa  # Fix for FutureWarning
from scipy.stats import pearsonr

# ==========================================
# 🛠️ CONFIGURATION & LOGGING (UTF-8 SAFE)
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        # Forced UTF-8 to handle emojis in the log file
        logging.FileHandler("pipeline_execution.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 🛡️ DATA CONTRACTS (SCHEMAS) - UPDATED
# ==========================================

WEATHER_SCHEMA = pa.DataFrameSchema({
    "fecha": pa.Column(str, checks=pa.Check.str_matches(r'\d{4}-\d{2}-\d{2}')),
    "tmax": pa.Column(float, checks=pa.Check.in_range(-5, 55), coerce=True), # coerce=True soluciona el error
    "hrMin": pa.Column(float, checks=pa.Check.in_range(0, 100), coerce=True), # coerce=True soluciona el error
})

ISTAC_SCHEMA = pa.DataFrameSchema({
    "ISLA": pa.Column(str),
    "ALOJ_CATEG": pa.Column(str),
    "ANTELACION_VIAJE": pa.Column(str),
    "OLA": pa.Column(int, checks=pa.Check.in_range(1, 12), coerce=True)
})

# ==========================================
# ⚙️ CORE PIPELINE FUNCTIONS
# ==========================================

def process_weather_data(file_path: Path) -> pd.DataFrame:
    logger.info(f"Phase 1: Ingesting Weather Data from {file_path}")
    
    # Senior fix: Handle European decimals and auto-detect separators
    df = pd.read_csv(file_path, sep=None, decimal=',', engine='python')
    df.columns = df.columns.str.strip()
    
    # Force numeric conversion to clear any 'str' remnants before validation
    df['tmax'] = pd.to_numeric(df['tmax'], errors='coerce')
    df['hrMin'] = pd.to_numeric(df['hrMin'], errors='coerce')
    
    # --- DATA QUALITY GATE ---
    try:
        WEATHER_SCHEMA.validate(df)
        logger.info("Weather Data Contract Verified.")
    except pa.errors.SchemaError as e:
        logger.critical(f"WEATHER DATA BREACH: {e}")
        raise

    # Feature Engineering
    df['date'] = pd.to_datetime(df['fecha'])
    df['MONTH_WAVE'] = df['date'].dt.month
    df['MONTHLY_AVG_TMAX'] = df.groupby('MONTH_WAVE')['tmax'].transform('mean')
    
    df['IS_CALIMA'] = (
        (df['tmax'] >= df['MONTHLY_AVG_TMAX'] + 4.5) & 
        (df['hrMin'] <= 55.0)
    ).astype(int)

    return df.groupby('MONTH_WAVE')['IS_CALIMA'].sum().reset_index().rename(
        columns={'IS_CALIMA': 'ACTUAL_CALIMA_DAYS'}
    )

def process_tourism_data(raw_path: Path, files: list) -> pd.DataFrame:
    logger.info("Phase 2: Processing Booking Microdata (ISTAC)...")
    dfs_list = []
    for file_name in files:
        path = raw_path / file_name
        if not path.exists():
            continue
            
        temp_df = pd.read_csv(path, sep=',', encoding='latin-1', low_memory=False)
        temp_df.columns = temp_df.columns.str.strip().str.upper()
        
        try:
            ISTAC_SCHEMA.validate(temp_df)
            dfs_list.append(temp_df)
        except pa.errors.SchemaError as e:
            logger.error(f"SCHEMA DRIFT in {file_name}: {e}")
            continue

    if not dfs_list:
        raise ValueError("No valid ISTAC data collected.")

    full_tourists = pd.concat(dfs_list, ignore_index=True)
    
    final_df = full_tourists[
        (full_tourists['ISLA'] == 'ES705') & 
        (full_tourists['ALOJ_CATEG'] == 'HOTEL_ESTRELLAS_4')
    ].copy()
    
    final_df['LAST_MINUTE_NUM'] = final_df['ANTELACION_VIAJE'].isin(['D_LT1', 'D1T15']).astype(int)

    return final_df.groupby('OLA').agg(
        LAST_MINUTE_RATIO=('LAST_MINUTE_NUM', 'mean'),
        TOTAL_SAMPLES=('LAST_MINUTE_NUM', 'size')
    ).reset_index().rename(columns={'OLA': 'MONTH_WAVE'})

# ==========================================
# 🚀 MAIN EXECUTION
# ==========================================

def run_pipeline():
    # Emojis removed from console logs to prevent UnicodeEncodeError on Windows
    logger.info("STARTING DATA PIPELINE (v2.2 - Windows Compatibility)")
    
    BASE_DIR = Path(__file__).resolve().parent.parent
    RAW_DATA_DIR = BASE_DIR / 'data' / 'raw'
    
    try:
        monthly_weather = process_weather_data(RAW_DATA_DIR / 'weather_data_2025.csv')
        monthly_istac = process_tourism_data(RAW_DATA_DIR, [
            "First_Quarter_2025.csv", "Second_Quarter_2025.csv", 
            "Third_Quarter_2025.csv", "Fourth_Quarter_2025.csv"
        ])

        report = pd.merge(monthly_istac, monthly_weather, on='MONTH_WAVE', how='left')
        report['ACTUAL_CALIMA_DAYS'] = report['ACTUAL_CALIMA_DAYS'].fillna(0).astype(int)
        report['% LAST MINUTE'] = (report['LAST_MINUTE_RATIO'] * 100).round(2)

        # Pearson Correlation and P-Value
        r_coeff, p_value = pearsonr(report['ACTUAL_CALIMA_DAYS'], report['LAST_MINUTE_RATIO'])

        print("\n" + "="*70)
        print(" EXECUTIVE REVENUE ANALYTICS REPORT")
        print("="*70)
        print(report[['MONTH_WAVE', 'ACTUAL_CALIMA_DAYS', '% LAST MINUTE', 'TOTAL_SAMPLES']].to_string(index=False))
        print("-"*70)
        print(f"PEARSON CORRELATION (r): {r_coeff:.4f} (p-value: {p_value:.4f})")
        print("="*70 + "\n")

    except Exception as e:
        logger.error(f"PIPELINE CRASHED: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()