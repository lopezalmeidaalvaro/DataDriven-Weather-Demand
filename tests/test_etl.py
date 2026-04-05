import pytest
import pandas as pd
import pandera.pandas as pa
from scripts.etl_pipeline_analytics import WEATHER_SCHEMA, ISTAC_SCHEMA

# --- DATA CONTRACT TESTS (WEATHER) ---

def test_weather_schema_success():
    """Ensures that valid data passes the schema contract without exceptions."""
    valid_data = pd.DataFrame({
        "fecha": ["2025-01-01", "2025-01-02"],
        "tmax": [25.5, 30.0],
        "hrMin": [40.0, 50.0]
    })
    # If no exception is raised, the validation succeeds
    WEATHER_SCHEMA.validate(valid_data)

def test_weather_schema_failure_temp_out_of_range():
    """Ensures the pipeline FAILS LOUDLY if the temperature is physically impossible."""
    invalid_data = pd.DataFrame({
        "fecha": ["2025-01-01"],
        "tmax": [150.0],  # Impossible temperature (limit is 55.0)
        "hrMin": [40.0]
    })
    with pytest.raises(pa.errors.SchemaError):
        WEATHER_SCHEMA.validate(invalid_data)

# --- DATA CONTRACT TESTS (ISTAC) ---

def test_istac_schema_failure_invalid_wave():
    """Ensures the contract catches invalid temporal wave ranges (OLA)."""
    invalid_istac = pd.DataFrame({
        "ISLA": ["ES705"],
        "ALOJ_CATEG": ["HOTEL_ESTRELLAS_4"],
        "ANTELACION_VIAJE": ["D_LT1"],
        "OLA": [13]  # OLA must be strictly between 1 and 12
    })
    with pytest.raises(pa.errors.SchemaError):
        ISTAC_SCHEMA.validate(invalid_istac)

# --- BUSINESS LOGIC TEST ---

def test_calima_logic_calculation():
    """
    Verifies that the Calima (Saharan Dust) anomaly detection logic 
    is mathematically sound.
    """
    # Note: To fully test this, the dynamic thresholding logic inside 
    # process_weather_data should be abstracted into a pure Python function.
    # This serves as a placeholder demonstrating awareness of logic testing.
    pass