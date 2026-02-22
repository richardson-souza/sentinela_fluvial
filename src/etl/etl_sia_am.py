
import pandas as pd
import numpy as np
import logging
import os
import gc
import sys
from typing import Dict, Union, Any, List
from pysus.online_data import SIA # type: ignore

# Ensure project root is in sys.path for direct execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src import config

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_seasonality(month: Union[str, int, float]) -> str:
    """
    Calculates the Amazonian season based on the month.
    
    Args:
        month: The month number (1-12) as integer, string, or float.
        
    Returns:
        str: "Vazante" (Ebb) or "Cheia" (Flood) or "Indeterminado" (Indeterminate).
    """
    if pd.isna(month): return "Indeterminado"
    try:
        m = int(month)
        return "Vazante" if m in [7, 8, 9, 10, 11] else "Cheia"
    except (ValueError, TypeError):
        return "Indeterminado"

def load_procedure_names() -> Dict[str, str]:
    """
    Loads procedure names from the Unified Table.
    
    Returns:
        Dict[str, str]: Dictionary mapping procedure codes to names.
    """
    if not os.path.exists(config.PROCEDURE_TABLE_PATH):
        logging.warning("Unified Table not found. Using generic names.")
        return {code: "Unknown Procedure" for code in config.PRIMARY_CARE_MARKERS}
    
    proc_dict = {}
    try:
        with open(config.PROCEDURE_TABLE_PATH, 'r', encoding='latin-1') as f:
            for line in f:
                code = line[0:10].strip()
                if code in config.PRIMARY_CARE_MARKERS:
                    proc_dict[code] = line[10:260].strip()
    except Exception as e:
        logging.error(f"Error loading procedure names: {e}")
        return {code: "Error Loading Name" for code in config.PRIMARY_CARE_MARKERS}
        
    return proc_dict

def process_sia_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a chunk of SIA data, handling column name variations and filtering.
    
    Args:
        df (pd.DataFrame): Raw SIA dataframe chunk.
        
    Returns:
        pd.DataFrame: Processed and filtered dataframe.
    """
    # Normalize column names (remove PA_ prefix if exists)
    df.columns = [c.replace('PA_', '') for c in df.columns]
    
    if 'PROC_ID' not in df.columns:
        return pd.DataFrame()

    # Filter Procedures
    df['PROC_ID'] = df['PROC_ID'].astype(str).str.strip()
    df_filtered = df[df['PROC_ID'].isin(config.PRIMARY_CARE_MARKERS)].copy()
    
    if df_filtered.empty:
        return df_filtered

    # Clean CIDPRI
    if 'CIDPRI' in df_filtered.columns:
        df_filtered['CIDPRI'] = df_filtered['CIDPRI'].astype(str).str.strip().str.upper()

    # Extract Month for Seasonality
    if 'DT_ATEND' in df_filtered.columns:
        df_filtered['MONTH_STR'] = df_filtered['DT_ATEND'].astype(str).str.strip().str[4:6]
    elif 'CMP' in df_filtered.columns:
        df_filtered['MONTH_STR'] = df_filtered['CMP'].astype(str).str.strip().str[-2:]
    else:
        df_filtered['MONTH_STR'] = np.nan

    df_filtered['AMAZON_SEASON'] = df_filtered['MONTH_STR'].apply(check_seasonality)
    # Mapping back to Portuguese column name for consistency with output requirements if needed, 
    # but English variable names preferred in code. Keeping output column 'ESTACAO_AMAZONICA' for compatibility?
    # The requirement is "recriando os scripts... usando a lingua inglesa como padrão". 
    # Usually data columns might need to stay compatible with downstream tools. 
    # I will keep output column names consistent with original script to avoid breaking dashboards, but use English variable names.
    df_filtered.rename(columns={'AMAZON_SEASON': 'ESTACAO_AMAZONICA'}, inplace=True)
    
    return df_filtered

def main() -> None:
    try:
        proc_names = load_procedure_names()
        local_file = os.path.join(config.RAW_DATA_DIR, f'SIA_{config.STATE}_{config.YEAR}.csv')
        
        chunk_list: List[pd.DataFrame] = []

        if os.path.exists(local_file):
            logging.info(f"Processing local file {local_file}...")
            # Using iterator for large files
            for chunk in pd.read_csv(local_file, dtype=str, chunksize=100000, low_memory=False):
                processed = process_sia_chunk(chunk)
                if not processed.empty:
                    chunk_list.append(processed)
            
            if not chunk_list:
                logging.warning("No marker procedures found in local file.")
                return
                
            df_final = pd.concat(chunk_list, ignore_index=True)
        else:
            logging.info("Local file not found. Downloading via PySUS (BI group)...")
            data = SIA.download(states=config.STATE, years=config.YEAR, months=config.MONTHS, groups='BI')
            
            dfs = []
            data_list = data if isinstance(data, list) else [data]
            for d in data_list:
                processed = process_sia_chunk(d.to_dataframe())
                if not processed.empty:
                    dfs.append(processed)
            
            if not dfs:
                 logging.warning("No marker procedures found in downloaded data.")
                 return

            df_final = pd.concat(dfs, ignore_index=True)

        # Enrichment
        df_final['NO_PROCEDIMENTO'] = df_final['PROC_ID'].map(proc_names)
        
        # Final Column Selection
        cols_to_keep = [
            'CODUNI', 'UFMUN', 'DT_ATEND', 'CMP', 'PROC_ID', 
            'NO_PROCEDIMENTO', 'CIDPRI', 'QT_APROV', 'VL_APROV', 'ESTACAO_AMAZONICA'
        ]
        final_cols = [c for c in cols_to_keep if c in df_final.columns]
        df_fact = df_final[final_cols].copy()

        logging.info(f"Saving final artifact: {config.OUTPUT_FILE_AMBULATORY}")
        df_fact.to_csv(config.OUTPUT_FILE_AMBULATORY, index=False)
        
        # Statistics
        logging.info("=== SIA ETL Process Summary ===")
        logging.info(f"Total Primary Care production records: {len(df_fact)}")
        if 'CIDPRI' in df_fact.columns:
            logging.info(f"Top 5 CIDPRI:\n{df_fact['CIDPRI'].value_counts().head(5)}")
        logging.info("===============================")

    except Exception as e:
        logging.error(f"Error in SIA ETL: {e}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()
