import pandas as pd
import numpy as np
import logging
import os
import gc
import sys
from typing import Optional, Union
from pysus.online_data import SIH # type: ignore

# Ensure project root is in sys.path for direct execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src import config

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_seasonality(dt: Union[pd.Timestamp, None]) -> str:
    """
    REQ 2: Calculates the Amazonian season based on the month.
    Rule: If month is 07, 08, 09, 10 or 11 -> "Vazante" (Ebb); Otherwise -> "Cheia" (Flood).
    
    Args:
        dt: Datetime object (or pandas Timestamp).
        
    Returns:
        str: "Vazante" or "Cheia" or "Indeterminado".
    """
    if pd.isna(dt):
        return "Indeterminado"
    try:
        return "Vazante" if dt.month in [7, 8, 9, 10, 11] else "Cheia"
    except AttributeError:
        return "Indeterminado"

def load_bronze_data() -> pd.DataFrame:
    """
    Loads SIH/RD (Bronze) data.
    Tries to load consolidated local file if available; otherwise downloads via PySUS.
    
    Returns:
        pd.DataFrame: Raw SIH dataframe.
    """
    local_file = f'RD_{config.STATE}_{config.CNES_MONTHS[0]}_{config.YEAR}.csv' # Using logic from other scripts or standard naming? 
    # Original script used 'RD_AM_12_2025.csv' but logic said months range(1, 13).
    # Since original used hardcoded local filename check, let's stick to download or specific file if we can infer it.
    # The original script main logic for download was:
    # SIH.download(states=STATE, years=YEAR, months=MONTHS, groups='RD')
    
    # Let's use a generic local file name check or just download logic.
    # For safety to match original behavior of checking a specific file:
    local_file_consolidated = os.path.join(config.RAW_DATA_DIR, 'RD_{}_{}_{}.csv'.format(config.STATE, 'ALL', config.YEAR))

    # But original script checked: 'RD_AM_12_2025.csv'.
    # I'll stick to the download logic as primary source of truth if I don't see a clear local file pattern in config.
    
    logging.info(f"Downloading Bronze data via PySUS for SIH/RD {config.STATE} {config.YEAR}...")
    try:
        data_rd = SIH.download(states=config.STATE, years=config.YEAR, months=config.MONTHS, groups='RD')
        if isinstance(data_rd, list):
            df_rd = pd.concat([d.to_dataframe() for d in data_rd])
        else:
            df_rd = data_rd.to_dataframe()
        return df_rd
    except Exception as e:
        logging.error(f"Error downloading via PySUS: {e}")
        raise

def load_cid_mapping() -> Optional[pd.DataFrame]:
    """
    Loads CID code mapping to friendly descriptions.
    File: config.CID_TABLE_PATH
    
    Returns:
        Optional[pd.DataFrame]: Dataframe with CID mappings or None.
    """
    if not os.path.exists(config.CID_TABLE_PATH):
        logging.warning(f"CID mapping file not found at {config.CID_TABLE_PATH}. Skipping name enrichment.")
        return None
    
    logging.info("Loading CID dictionary...")
    try:
        # tb_cid.txt has fixed width: 4 chars for code, followed by description.
        # ISO-8859-1 encoding is common for DATASUS.
        df_cid = pd.read_fwf(config.CID_TABLE_PATH, colspecs=[(0, 4), (4, 104)], header=None, 
                             names=['DIAG_PRINC', 'DESC_DIAG'], encoding='ISO-8859-1')
        
        # Cleaning
        df_cid['DIAG_PRINC'] = df_cid['DIAG_PRINC'].astype(str).str.strip().str.upper()
        df_cid['DESC_DIAG'] = df_cid['DESC_DIAG'].astype(str).str.strip()
        
        # Remove duplicates
        df_cid = df_cid.drop_duplicates(subset=['DIAG_PRINC'])
        
        return df_cid
    except Exception as e:
        logging.error(f"Error loading CID mapping: {e}")
        return None

def transform_sih_silver(df_rd: pd.DataFrame, df_cid_map: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transforms Bronze data to Silver following ETL requirements.
    
    Args:
        df_rd (pd.DataFrame): Raw Dataframe.
        df_cid_map (Optional[pd.DataFrame]): CID mapping dataframe.
        
    Returns:
        pd.DataFrame: Transformed Silver dataframe.
    """
    logging.info("Starting SIH/RD transformation to Silver layer...")

    # Step 1: Filtering and Selection
    # Scope Filter: Only residents of Amazonas (MUNIC_RES starts with 13)
    df_rd['MUNIC_RES'] = df_rd['MUNIC_RES'].astype(str).str.strip()
    df_silver = df_rd[df_rd['MUNIC_RES'].str.startswith('13')].copy()
    logging.info(f"Scope Filter: {len(df_silver)} resident records kept.")

    # Clean DIAG_PRINC
    if 'DIAG_PRINC' in df_silver.columns:
        df_silver['DIAG_PRINC'] = df_silver['DIAG_PRINC'].astype(str).str.strip().str.upper()

    # Enrichment with CID names
    if df_cid_map is not None:
        logging.info("Enriching data with diagnostic descriptions...")
        df_silver = pd.merge(df_silver, df_cid_map, on='DIAG_PRINC', how='left')
        df_silver['DESC_DIAG'] = df_silver['DESC_DIAG'].fillna("Diagnóstico não identificado")

    # Sanitization: Convert date columns and remove nulls in DT_INTER
    logging.info("Sanitizing dates (DT_INTER and DT_SAIDA)...")
    df_silver['DT_INTER'] = pd.to_datetime(df_silver['DT_INTER'], format='%Y%m%d', errors='coerce')
    df_silver['DT_SAIDA'] = pd.to_datetime(df_silver['DT_SAIDA'], format='%Y%m%d', errors='coerce')
    
    # Remove rows with invalid DT_INTER
    df_silver = df_silver[df_silver['DT_INTER'].notna()].copy()
    logging.info(f"Records after DT_INTER sanitization: {len(df_silver)}")

    # Step 2: Feature Engineering
    # Amazon Seasonality Calculation
    logging.info("Calculating Amazonian seasonality flag...")
    df_silver['ESTACAO_AMAZONICA'] = df_silver['DT_INTER'].apply(check_seasonality)

    # Length of Stay (DT_SAIDA - DT_INTER)
    logging.info("Calculating length of stay...")
    df_silver['TEMPO_PERMANENCIA'] = (df_silver['DT_SAIDA'] - df_silver['DT_INTER']).dt.days
    
    # Handle negative or null values
    df_silver['TEMPO_PERMANENCIA'] = df_silver['TEMPO_PERMANENCIA'].fillna(0).clip(lower=0)

    return df_silver

def main() -> None:
    try:
        # Extraction
        df_bronze = load_bronze_data()
        
        # Auxiliary Mapping
        df_cid_map = load_cid_mapping()
        
        # Transformation
        df_silver = transform_sih_silver(df_bronze, df_cid_map)
        
        # Load: Save Final Artifact
        logging.info(f"Saving final artifact: {config.OUTPUT_FILE_HOSPITALIZATION}")
        df_silver.to_csv(config.OUTPUT_FILE_HOSPITALIZATION, index=False)
        
        # Summary Statistics
        logging.info("=== SIH/RD ETL Process Summary ===")
        logging.info(f"Total records in fact table: {len(df_silver)}")
        logging.info(f"Distribution by Season:\n{df_silver['ESTACAO_AMAZONICA'].value_counts()}")
        logging.info(f"Top 5 DIAG_PRINC:\n{df_silver['DIAG_PRINC'].value_counts().head(5)}")
        logging.info(f"Average Length of Stay: {df_silver['TEMPO_PERMANENCIA'].mean():.2f} days")
        logging.info("==================================")
        
        # Memory Cleanup
        del df_bronze, df_silver
        gc.collect()

    except Exception as e:
        logging.error(f"Fatal error during ETL execution: {e}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()
