
import pandas as pd
import numpy as np
import logging
import os
import sys
from typing import Tuple
from pysus.online_data import CNES # type: ignore

# Ensure project root is in sys.path for direct execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src import config

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_bronze_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads Bronze layer data (ST and LT groups) for CNES.
    Downloads from PySUS if local files are missing.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Tuple containing (df_st, df_lt).
    """
    st_file = os.path.join(config.RAW_DATA_DIR, f'CNES_ST_{config.STATE}_{config.CNES_MONTHS[0]}_{config.YEAR}.csv')
    lt_file = os.path.join(config.RAW_DATA_DIR, f'CNES_LT_{config.STATE}_{config.CNES_MONTHS[0]}_{config.YEAR}.csv')
    
    if os.path.exists(st_file) and os.path.exists(lt_file):
        logging.info("Loading Bronze data from local files...")
        df_st = pd.read_csv(st_file, dtype=str, low_memory=False)
        df_lt = pd.read_csv(lt_file, dtype=str, low_memory=False)
    else:
        logging.info(f"Downloading Bronze data via PySUS for {config.STATE} {config.YEAR}/{config.CNES_MONTHS}...")
        data_st = CNES.download(group='ST', states=config.STATE, years=config.YEAR, months=config.CNES_MONTHS)
        df_st = pd.concat([d.to_dataframe() for d in data_st]) if isinstance(data_st, list) else data_st.to_dataframe()
        
        data_lt = CNES.download(group='LT', states=config.STATE, years=config.YEAR, months=config.CNES_MONTHS)
        df_lt = pd.concat([d.to_dataframe() for d in data_lt]) if isinstance(data_lt, list) else data_lt.to_dataframe()
        
        # Save cache
        df_st.to_csv(st_file, index=False)
        df_lt.to_csv(lt_file, index=False)

    return df_st, df_lt

def transform_cnes_silver(df_st: pd.DataFrame, df_lt: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms Bronze data to Silver, enriching with establishment names and business rules.

    Args:
        df_st (pd.DataFrame): Dataframe for ST group (Establishments).
        df_lt (pd.DataFrame): Dataframe for LT group (Beds).

    Returns:
        pd.DataFrame: Transformed Silver dataframe.
    """
    logging.info("Starting transformation to Silver layer...")

    # Step 1: Filter and Select
    df_st['CODUFMUN'] = df_st['CODUFMUN'].astype(str).str.strip()
    # Filter for STATE (AM starts with 13)
    # Using config.STATE to determine prefix would be better if generalized, but hardcoded '13' matches original logic for AM.
    # Assuming config.STATE is 'AM', prefix is '13'.
    df_silver_st = df_st[df_st['CODUFMUN'].str.startswith('13')].copy()
    
    # Column Selection
    cols_st = ['CNES', 'CODUFMUN', 'TP_UNID', 'TPGESTAO', 'COD_CEP']
    for col in ['LATITUDE', 'LONGITUDE']:
        if col in df_silver_st.columns:
            cols_st.append(col)
    
    df_silver_st = df_silver_st[cols_st]

    # Enrichment Step: Establishment Names
    if os.path.exists(config.CNES_NAMES_CACHE_FILE):
        logging.info(f"Enriching with establishment names from {config.CNES_NAMES_CACHE_FILE}...")
        df_names = pd.read_csv(config.CNES_NAMES_CACHE_FILE, dtype=str)
        # Normalize CNES to 7 digits with leading zeros
        df_names['CNES'] = df_names['CNES'].str.strip().str.zfill(7)
        df_silver_st['CNES'] = df_silver_st['CNES'].str.strip().str.zfill(7)
        
        # Merge to bring FANTASIA column
        df_silver_st = pd.merge(df_silver_st, df_names[['CNES', 'FANTASIA']], on='CNES', how='left')
        df_silver_st.rename(columns={'FANTASIA': 'NO_ESTABELECIMENTO'}, inplace=True)
    else:
        logging.warning(f"CNES Names cache file {config.CNES_NAMES_CACHE_FILE} not found. Skipping name enrichment.")

    # Preparation of LT (Beds)
    df_lt['CNES'] = df_lt['CNES'].astype(str).str.strip().str.zfill(7)
    df_lt['QT_EXIST'] = pd.to_numeric(df_lt['QT_EXIST'], errors='coerce').fillna(0)
    df_lt['QT_SUS'] = pd.to_numeric(df_lt['QT_SUS'], errors='coerce').fillna(0)

    # Aggregate beds by CNES
    df_leitos_agg = df_lt.groupby('CNES').agg({
        'QT_EXIST': 'sum',
        'QT_SUS': 'sum'
    }).reset_index()

    # Join ST + LT
    df_dim = pd.merge(df_silver_st, df_leitos_agg, on='CNES', how='left')
    df_dim[['QT_EXIST', 'QT_SUS']] = df_dim[['QT_EXIST', 'QT_SUS']].fillna(0)

    # Step 2: Business Rules
    # Check if Fluvial UBS (TP_UNID == '73')
    df_dim['IS_UBS_FLUVIAL'] = np.where(df_dim['TP_UNID'].astype(str).str.strip() == '73', 1, 0)
    
    df_dim['TPGESTAO'] = df_dim['TPGESTAO'].astype(str).str.strip()
    
    # Calculate Real SUS Capacity
    # If Management is Municipal (M) or State (E) and QT_SUS is 0, assume all existing are SUS.
    df_dim['CAPACIDADE_REAL_SUS'] = np.where(
        (df_dim['TPGESTAO'].isin(['M', 'E'])) & (df_dim['QT_SUS'] == 0),
        df_dim['QT_EXIST'],
        df_dim['QT_SUS']
    )

    return df_dim

def main() -> None:
    try:
        df_st, df_lt = load_bronze_data()
        df_dim_estabelecimentos = transform_cnes_silver(df_st, df_lt)
        
        logging.info(f"Saving final artifact: {config.OUTPUT_FILE_ESTABLISHMENTS}")
        df_dim_estabelecimentos.to_csv(config.OUTPUT_FILE_ESTABLISHMENTS, index=False)
        
        logging.info(f"ETL Completed. Total Establishments: {len(df_dim_estabelecimentos)}")
        if 'NO_ESTABELECIMENTO' in df_dim_estabelecimentos.columns:
            logging.info(f"Sample with names:\n{df_dim_estabelecimentos[['CNES', 'NO_ESTABELECIMENTO']].head()}")
        else:
            logging.info(f"Sample:\n{df_dim_estabelecimentos[['CNES']].head()}")

    except Exception as e:
        logging.error(f"Error in ETL process: {e}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()
