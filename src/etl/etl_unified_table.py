
import os
import logging
import zipfile
from ftplib import FTP
import sys
from typing import Optional

# Ensure project root is in sys.path for direct execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src import config

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_unified_table() -> Optional[str]:
    """
    Downloads the Unified Table ZIP file from the SUS FTP server.

    Returns:
        Optional[str]: Path to the downloaded local file, or None if failed.
    """
    if os.path.exists(config.LOCAL_ZIP_UNIFIED):
        logging.info(f"Local file {config.LOCAL_ZIP_UNIFIED} already exists. Skipping download.")
        return config.LOCAL_ZIP_UNIFIED

    logging.info(f"Connecting to {config.FTP_UNIFIED_HOST}...")
    try:
        ftp = FTP(config.FTP_UNIFIED_HOST)
        ftp.login()
        ftp.cwd(config.FTP_UNIFIED_DIR)
        
        logging.info(f"Downloading {config.REMOTE_ZIP_UNIFIED} to {config.LOCAL_ZIP_UNIFIED}...")
        with open(config.LOCAL_ZIP_UNIFIED, "wb") as f:
            ftp.retrbinary(f"RETR {config.REMOTE_ZIP_UNIFIED}", f.write)
        
        ftp.quit()
        logging.info("Download completed successfully.")
        return config.LOCAL_ZIP_UNIFIED
        
    except Exception as e:
        logging.error(f"Error downloading Unified Table: {e}")
        if os.path.exists(config.LOCAL_ZIP_UNIFIED):
            os.remove(config.LOCAL_ZIP_UNIFIED) # Clean up partial file
        return None

def extract_unified_table(zip_path: str) -> bool:
    """
    Extracts required files (tb_procedimento.txt, tb_cid.txt) from the Unified Table ZIP.

    Args:
        zip_path (str): Path to the ZIP file.

    Returns:
        bool: True if extraction was successful, False otherwise.
    """
    if not os.path.exists(zip_path):
        logging.error(f"Zip file {zip_path} not found.")
        return False

    if not os.path.exists(config.UNIFIED_TABLE_DIR):
        os.makedirs(config.UNIFIED_TABLE_DIR)

    files_to_extract = ['tb_procedimento.txt', 'tb_cid.txt']
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Check for files inside the zip (sometimes they are inside a folder or root)
            # The original structure usually contains these files directly or in a folder.
            # We will search for them.
            
            all_files = z.namelist()
            extracted_count = 0
            
            for target_file in files_to_extract:
                # Simple case: file is at root
                if target_file in all_files:
                    logging.info(f"Extracting {target_file}...")
                    z.extract(target_file, path=config.UNIFIED_TABLE_DIR)
                    extracted_count += 1
                    continue
                
                # Case insensitive search or search in subfolders
                found = False
                for name in all_files:
                    if name.lower().endswith(target_file.lower()):
                        logging.info(f"Found {name}. Extracting to {config.UNIFIED_TABLE_DIR}...")
                        # Extract and rename if needed, but zipfile.extract keeps structure.
                        # We might want to flatten it or just let it extract and config points to it.
                        # Since config.PROCEDURE_TABLE_PATH uses os.path.join(UNIFIED_TABLE_DIR, 'tb_procedimento.txt'),
                        # we prefer the file to be directly in UNIFIED_TABLE_DIR.
                        
                        source = z.open(name)
                        target_path = os.path.join(config.UNIFIED_TABLE_DIR, target_file) # Flatten
                        with open(target_path, "wb") as f:
                            f.write(source.read())
                        found = True
                        extracted_count += 1
                        break
                
                if not found:
                    logging.warning(f"File {target_file} not found in archive.")

            return extracted_count > 0

    except Exception as e:
        logging.error(f"Error extracting Unified Table: {e}")
        return False

def main() -> None:
    zip_file = download_unified_table()
    if zip_file:
         success = extract_unified_table(zip_file)
         if success:
             logging.info("Unified Table ETL completed successfully.")
         else:
             logging.error("Unified Table ETL failed during extraction.")

if __name__ == "__main__":
    main()
