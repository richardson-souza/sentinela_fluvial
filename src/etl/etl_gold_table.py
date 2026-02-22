import pandas as pd
import numpy as np
import logging
import gc
import os
import sys

# Ensure project root is in sys.path for direct execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src import config

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Aggregation Keys
KEY_COLS = ['CNES', 'COMPETENCIA', 'ESTACAO_AMAZONICA']


def get_predominant(x: pd.Series) -> str:
    """
    Identifies the predominant value (mode) in a series.
    
    Args:
        x (pd.Series): Input series.
        
    Returns:
        str: The most frequent value or 'N/A' if empty/null.
    """
    return x.value_counts().index[0] if not x.empty and x.notna().any() else "N/A"


def get_weak_signals(group: pd.Series) -> str:
    """
    Identifies emerging outbreaks or low-frequency sentinel diseases.
    
    Args:
        group (pd.Series): Series of diagnosis codes/descriptions.
        
    Returns:
        str: Alert message if sentinel disease found, otherwise 'Stable' or secondary diagnosis.
    """
    if group.empty:
        return "None"
    
    # 1. Priority: Sentinel Diseases (Even with few cases)
    # Mapping codes to English names for the alert
    sentinels = {
        'A00': 'Cholera',
        'B50': 'Malaria Falciparum',
        'B51': 'Malaria Vivax',
        'A27': 'Leptospirosis',
        'A01': 'Typhoid Fever'
    }
    
    counts = group.value_counts()
    for code, name in sentinels.items():
        # Check if the code (or its start) is present in the data
        match = [idx for idx in counts.index if str(idx).startswith(code)]
        if match:
            return f"ALERT: Case(s) of {name}"

    # 2. Alternative: Second most frequent diagnosis (Weak Signal)
    if len(counts) > 1:
        second_diag = counts.index[1]
        # Only report if not "N/A"
        if second_diag != "N/A" and counts.iloc[1] > 0:
            return f"Emerging: {second_diag}"
    
    return "Stable"


def build_gold_table() -> None:
    """
    Constructs the Gold Layer table (Analytical Base Table) by joining
    processed Hospitalization (SIH) and Ambulatory (SIA) data with
    Establishment and Municipality dimensions.
    """
    logging.info("Starting Gold Table construction (Gold Layer)...")

    # 1. Load Dimensions
    logging.info("Loading Establishment Dimension...")
    if not os.path.exists(config.OUTPUT_FILE_ESTABLISHMENTS):
        logging.error(f"File not found: {config.OUTPUT_FILE_ESTABLISHMENTS}")
        return

    df_dim_est = pd.read_csv(
        config.OUTPUT_FILE_ESTABLISHMENTS,
        dtype={'CNES': str, 'CODUFMUN': str}
    )
    
    logging.info("Loading Municipality Dimension...")
    if not os.path.exists(config.OUTPUT_FILE_MUNICIPALITIES):
        logging.error(f"File not found: {config.OUTPUT_FILE_MUNICIPALITIES}")
        return

    df_dim_mun = pd.read_csv(
        config.OUTPUT_FILE_MUNICIPALITIES,
        dtype={'CODUFMUN': str}
    )

    # 2. Process Hospitalization Facts (Demand)
    logging.info("Processing Hospitalization Facts (IS_HIDRICA)...")
    if not os.path.exists(config.OUTPUT_FILE_HOSPITALIZATION):
        logging.error(f"File not found: {config.OUTPUT_FILE_HOSPITALIZATION}")
        return

    df_sih = pd.read_csv(
        config.OUTPUT_FILE_HOSPITALIZATION,
        dtype={'CNES': str, 'DIAG_PRINC': str, 'DESC_DIAG': str},
        low_memory=False
    )
    
    # Extract Competence (YYYY-MM)
    if 'DT_INTER' in df_sih.columns:
        df_sih['COMPETENCIA'] = df_sih['DT_INTER'].str[:7]
    else:
        logging.warning("DT_INTER column missing in SIH data.")
        return
    
    # Demand Aggregation
    # Simplification for A00-A09 (Water-borne diseases)
    df_sih['IS_HIDRICA'] = df_sih['DIAG_PRINC'].str.startswith('A0') 
    
    sih_agg = df_sih.groupby(KEY_COLS).agg(
        TOTAL_INTERNACOES=('N_AIH', 'count'),
        INTERNACOES_HIDRICAS=('IS_HIDRICA', 'sum'),
        PERMANENCIA_MEDIA=('TEMPO_PERMANENCIA', 'mean'),
        DIAGNOSTICO_PREDOMINANTE=('DESC_DIAG', get_predominant),
        SINAIS_FRACOS=('DESC_DIAG', get_weak_signals)
    ).reset_index()

    # Identify specifically the predominant water-borne disease
    hidrica_agg = df_sih[df_sih['IS_HIDRICA']].groupby(KEY_COLS).agg(
        DOENCA_HIDRICA_PREDOMINANTE=('DESC_DIAG', get_predominant)
    ).reset_index()
    
    sih_agg = pd.merge(
        sih_agg,
        hidrica_agg,
        on=KEY_COLS,
        how='left'
    ).fillna({'DOENCA_HIDRICA_PREDOMINANTE': 'None'})
    
    del df_sih, hidrica_agg
    gc.collect()

    # 3. Process Production Facts (Prevention)
    logging.info("Processing Ambulatory Production Facts...")
    if not os.path.exists(config.OUTPUT_FILE_AMBULATORY):
        logging.error(f"File not found: {config.OUTPUT_FILE_AMBULATORY}")
        return

    df_sia = pd.read_csv(
        config.OUTPUT_FILE_AMBULATORY,
        dtype={'CODUNI': str, 'DT_ATEND': str, 'NO_PROCEDIMENTO': str}
    )
    
    # Normalize CNES and Competence
    df_sia.rename(columns={'CODUNI': 'CNES'}, inplace=True)
    
    # Check if 'DT_ATEND' exists and format it
    if 'DT_ATEND' in df_sia.columns:
        # Assuming YYYYMM format from source, transform to YYYY-MM
        df_sia['COMPETENCIA'] = (
            df_sia['DT_ATEND'].astype(str).str.strip().str[:4] + '-' + 
            df_sia['DT_ATEND'].astype(str).str.strip().str[4:6]
        )
    else:
        logging.warning("DT_ATEND column missing in SIA data.")
        return
    
    # Prevention Aggregation
    df_sia['QT_APROV'] = pd.to_numeric(df_sia['QT_APROV'], errors='coerce').fillna(0)
    
    sia_agg = df_sia.groupby(KEY_COLS).agg(
        TOTAL_PRODUCAO_AP=('QT_APROV', 'sum'),
        PROCEDIMENTO_AP_PREDOMINANTE=('NO_PROCEDIMENTO', get_predominant)
    ).reset_index()
    
    del df_sia
    gc.collect()

    # 4. Final Join (Join Strategy)
    logging.info("Joining tables (Join Strategy)...")
    
    # Start by merging the two fact tables
    df_fatos = pd.merge(sih_agg, sia_agg, on=KEY_COLS, how='outer').fillna({
        'TOTAL_INTERNACOES': 0,
        'INTERNACOES_HIDRICAS': 0,
        'TOTAL_PRODUCAO_AP': 0,
        'DIAGNOSTICO_PREDOMINANTE': 'N/A',
        'DOENCA_HIDRICA_PREDOMINANTE': 'None',
        'SINAIS_FRACOS': 'Stable',
        'PROCEDIMENTO_AP_PREDOMINANTE': 'N/A'
    })
    
    # Join with Establishment Dimension
    df_gold = pd.merge(df_fatos, df_dim_est, on='CNES', how='left')
    
    # Join with Municipality Dimension to translate CODUFMUN
    logging.info("Enriching with municipality names...")
    if 'CODUFMUN' in df_gold.columns:
        df_gold = pd.merge(
            df_gold,
            df_dim_mun[['CODUFMUN', 'NO_MUNICIPIO']],
            on='CODUFMUN',
            how='left'
        )
    
    # Quality Filter: Remove records without identification (Null Names)
    initial_rows = len(df_gold)
    df_gold = df_gold.dropna(subset=['NO_MUNICIPIO', 'NO_ESTABELECIMENTO'])
    dropped_rows = initial_rows - len(df_gold)
    if dropped_rows > 0:
        logging.warning(
            f"Data Quality: Removed {dropped_rows} records with incomplete registration info."
        )

    # 5. Feature Engineering: System Pressure
    logging.info("Calculating system pressure indicators...")
    
    # Calculate Estimated Occupancy Rate
    # Handle division by zero or null capacity
    df_gold['CAPACIDADE_REAL_SUS'] = pd.to_numeric(
        df_gold['CAPACIDADE_REAL_SUS'], errors='coerce'
    ).fillna(0)
    
    df_gold['TAXA_OCUPACAO_ESTIMADA'] = np.where(
        df_gold['CAPACIDADE_REAL_SUS'] > 0,
        df_gold['TOTAL_INTERNACOES'] / df_gold['CAPACIDADE_REAL_SUS'],
        0
    )
    
    conditions = [
        (df_gold['TAXA_OCUPACAO_ESTIMADA'] > 1.5),
        (df_gold['TAXA_OCUPACAO_ESTIMADA'] > 0.8),
        (df_gold['TOTAL_PRODUCAO_AP'] > 0)
    ]
    choices = ['Critical', 'High', 'Normal']
    df_gold['STATUS_PRESSAO'] = np.select(
        conditions, choices, default='Indetermined'
    )

    # Reorder columns for better readability
    cols_order = [
        'COMPETENCIA', 'ESTACAO_AMAZONICA', 'NO_MUNICIPIO', 'NO_ESTABELECIMENTO',
        'TOTAL_INTERNACOES', 'DIAGNOSTICO_PREDOMINANTE', 'SINAIS_FRACOS',
        'INTERNACOES_HIDRICAS', 'DOENCA_HIDRICA_PREDOMINANTE',
        'TOTAL_PRODUCAO_AP', 'PROCEDIMENTO_AP_PREDOMINANTE',
        'STATUS_PRESSAO', 'IS_UBS_FLUVIAL', 'CAPACIDADE_REAL_SUS'
    ]
    
    current_cols = df_gold.columns.tolist()
    # Select columns that exist in the DataFrame, preserving order
    final_cols = [c for c in cols_order if c in current_cols] + \
                 [c for c in current_cols if c not in cols_order]
    
    df_gold = df_gold[final_cols]

    # 6. Save Final Artifact
    logging.info(f"Saving Gold Table: {config.OUTPUT_FILE_GOLD}")
    df_gold.to_csv(config.OUTPUT_FILE_GOLD, index=False)
    
    logging.info(f"Gold ETL Completed. Valid records: {len(df_gold)}")
    if 'STATUS_PRESSAO' in df_gold.columns:
        logging.info(
            f"Pressure Distribution:\n{df_gold['STATUS_PRESSAO'].value_counts()}"
        )


if __name__ == "__main__":
    build_gold_table()
