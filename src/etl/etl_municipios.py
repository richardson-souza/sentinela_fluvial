
import pandas as pd
import logging
import os
from ftplib import FTP
import zipfile
import sys
from typing import Optional

# Ensure project root is in sys.path for direct execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src import config

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_and_extract() -> Optional[str]:
    """
    Downloads the territorial ZIP file from FTP and extracts the municipality table.

    Returns:
        Optional[str]: The path to the extracted file, or None if download/extraction failed.
    """
    if not os.path.exists(config.LOCAL_ZIP_TERRITORIAL):
        logging.info(f"Connecting to {config.FTP_HOST}...")
        try:
            ftp = FTP(config.FTP_HOST)
            ftp.login()
            ftp.cwd(config.FTP_DIR)
            logging.info(f"Downloading {config.REMOTE_ZIP_TERRITORIAL}...")
            with open(config.LOCAL_ZIP_TERRITORIAL, "wb") as f:
                ftp.retrbinary(f"RETR {config.REMOTE_ZIP_TERRITORIAL}", f.write)
            ftp.quit()
        except Exception as e:
            logging.error(f"Download error: {e}")
            return None

    logging.info(f"Extracting {config.MUNICIPALITIES_TARGET_FILE} from {config.LOCAL_ZIP_TERRITORIAL}...")
    if not os.path.exists(config.TEMP_MUNIC_DIR):
        os.makedirs(config.TEMP_MUNIC_DIR)
        
    extracted_path = None
    with zipfile.ZipFile(config.LOCAL_ZIP_TERRITORIAL, 'r') as z:
        if config.MUNICIPALITIES_TARGET_FILE in z.namelist():
            z.extract(config.MUNICIPALITIES_TARGET_FILE, path=config.TEMP_MUNIC_DIR)
            extracted_path = os.path.join(config.TEMP_MUNIC_DIR, config.MUNICIPALITIES_TARGET_FILE)
            logging.info(f"File extracted: {extracted_path}")
        else:
            for name in z.namelist():
                if name.lower() == config.MUNICIPALITIES_TARGET_FILE.lower():
                    z.extract(name, path=config.TEMP_MUNIC_DIR)
                    extracted_path = os.path.join(config.TEMP_MUNIC_DIR, name)
                    logging.info(f"File located (case-insensitive) and extracted: {extracted_path}")
                    break
    return extracted_path

def transform_municipalities(file_path: str) -> None:
    """
    Processes the raw municipalities CSV and generates the dimension table.

    Args:
        file_path (str): Path to the raw CSV file.
    """
    logging.info(f"Processing {file_path}...")
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin-1', dtype=str)
        
        # Specific mapping based on detected columns:
        # ['CO_MUNICIP', 'CO_MUNICDV', 'CO_STATUS', 'CO_TIPO', 'DS_NOME', ...]
        rename_map = {
            'CO_MUNICIP': 'CODUFMUN',
            'DS_NOME': 'NO_MUNICIPIO'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Selection and Cleaning
        df_dim = df[['CODUFMUN', 'NO_MUNICIPIO']].dropna().copy()
        df_dim['CODUFMUN'] = df_dim['CODUFMUN'].str.strip()
        df_dim['NO_MUNICIPIO'] = df_dim['NO_MUNICIPIO'].str.strip()
        
        # Add UF Key
        df_dim['CODUF'] = df_dim['CODUFMUN'].str[:2]
        
        df_dim.to_csv(config.OUTPUT_FILE_MUNICIPALITIES, index=False)
        logging.info(f"Artifact {config.OUTPUT_FILE_MUNICIPALITIES} generated with {len(df_dim)} records.")
        
        # Validation for Manaus (130260)
        manaus = df_dim[df_dim['CODUFMUN'] == '130260']
        if not manaus.empty:
            logging.info(f"Validation: {manaus.iloc[0].to_dict()}")

    except Exception as e:
        logging.error(f"Transformation error: {e}")
        import traceback
        logging.error(traceback.format_exc())

def main() -> None:
    munic_file = download_and_extract()
    if munic_file:
        transform_municipalities(munic_file)

if __name__ == "__main__":
    main()
