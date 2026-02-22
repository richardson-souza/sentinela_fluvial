
import os

# Base Directories
DATA_DIR = 'data'
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
FINAL_DATA_DIR = os.path.join(DATA_DIR, 'final')

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(FINAL_DATA_DIR, exist_ok=True)

# Geographic Constants
STATE = 'AM'

# Temporal Constants
YEAR = 2025
MONTHS = list(range(1, 13))
CNES_MONTHS = [12]

# FTP Constants
FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/territorio/tabelas"
REMOTE_ZIP_TERRITORIAL = "10-base_territorial_out25.zip"
LOCAL_ZIP_TERRITORIAL = os.path.join(RAW_DATA_DIR, "base_territorial.zip")

# Unified Table FTP Constants
FTP_UNIFIED_HOST = "ftp2.datasus.gov.br"
FTP_UNIFIED_DIR = "/pub/sistemas/tup/downloads/"
REMOTE_ZIP_UNIFIED = "TabelaUnificada_202602_v2602121027.zip"
LOCAL_ZIP_UNIFIED = os.path.join(RAW_DATA_DIR, REMOTE_ZIP_UNIFIED)

# Directories
UNIFIED_TABLE_DIR = os.path.join(RAW_DATA_DIR, 'TabelaUnificada_202602_v2602121027')
TEMP_MUNIC_DIR = os.path.join(RAW_DATA_DIR, 'temp_munic')

# Input Data Paths
# Check if TabelaUnificada is in raw, else assume root for backward compatibility or update in next steps.
# To be safe with "raw files in data/raw", we should probably expect it there.
PROCEDURE_TABLE_PATH = os.path.join(UNIFIED_TABLE_DIR, 'tb_procedimento.txt') 
CID_TABLE_PATH = os.path.join(UNIFIED_TABLE_DIR, 'tb_cid.txt')
MUNICIPALITIES_TARGET_FILE = "tb_municip.csv"
CNES_NAMES_CACHE_FILE = os.path.join(RAW_DATA_DIR, 'CNES_NOMES_AM.csv')

# Output Files
OUTPUT_FILE_AMBULATORY = os.path.join(PROCESSED_DATA_DIR, 'fato_producao_ambulatorial.csv')
OUTPUT_FILE_HOSPITALIZATION = os.path.join(PROCESSED_DATA_DIR, 'fato_internacoes_sazonais.csv')
OUTPUT_FILE_ESTABLISHMENTS = os.path.join(PROCESSED_DATA_DIR, 'dim_estabelecimentos_am.csv')
OUTPUT_FILE_MUNICIPALITIES = os.path.join(PROCESSED_DATA_DIR, 'dim_municipios.csv')
OUTPUT_FILE_GOLD = os.path.join(PROCESSED_DATA_DIR, 'abt_monitoramento_territorial.csv')
OUTPUT_FILE_PROMPTS = os.path.join(FINAL_DATA_DIR, 'dataset_prompts_medgemma.jsonl')


# Primary Care Markers (Procedures)
PRIMARY_CARE_MARKERS = [
    '0301010110', # CONSULTA PRE-NATAL
    '0301100039', # AFERICAO DE PRESSAO ARTERIAL
    '0214010015', # GLICEMIA CAPILAR
    '0214010058', # TESTE RAPIDO PARA DETECCAO DE ANTICORPOS ANTI-HIV
    '0214010074', # TESTE RAPIDO TREPONEMICO (SIFILIS)
    '0301010064', # CONSULTA MEDICA EM ATENCAO PRIMARIA
    '0301010030', # CONSULTA DE PROFISSIONAIS DE NIVEL SUPERIOR NA ATENCAO PRIMARIA
    '0101030010', # VISITA DOMICILIAR POR PROFISSIONAL DE NIVEL MEDIO (ACS)
    '0301060037', # ATENDIMENTO DE URGENCIA EM ATENCAO BASICA
    '0202020452', # PESQUISA DE PLASMODIOS (MALARIA)
]
