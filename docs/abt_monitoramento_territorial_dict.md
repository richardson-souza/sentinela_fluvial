### Data Dictionary: Territorial Monitoring ABT (Analytical Base Table)

This document describes the features present in the `abt_monitoramento_territorial.csv` file, which serves as the Gold Layer for public health monitoring in the Amazon Rainforest. This table aggregates dimensional data with clinical facts to feed the MedGemma RAG pipeline.

#### Table Structure

| Column | Description | Data Type | Origin (Primary Source / Script) |
| :--- | :--- | :--- | :--- |
| **COMPETENCIA** | Reference month and year of the data (Format: YYYY-MM). | String | SIH (DT_INTER) and SIA (DT_ATEND) / `build_gold_table.py` |
| **ESTACAO_AMAZONICA** | Hydrological climate classification based on the month (Vazante/Ebb: Months 07 to 11; Cheia/Flood: remaining months). | String | Business Rule / `etl_sih_am.py` and `etl_sia_am.py` |
| **NO_MUNICIPIO** | Friendly name of the municipality in the state of Amazonas. | String | `dim_municipios.csv` (IBGE) / `etl_municipios.py` |
| **NO_ESTABELECIMENTO** | Trade name of the health facility. | String | `dim_estabelecimentos_am.csv` (CNES) / `etl_cnes_am.py` |
| **TOTAL_INTERNACOES** | Total number of hospital admissions registered during the competence month. | Float | SIH/RD (N_AIH) / `build_gold_table.py` |
| **DIAGNOSTICO_PREDOMINANTE** | Friendly name of the pathology (ICD-10) that generated the highest number of admissions in the period. | String | SIH/RD + Unified Table (`tb_cid.txt`) / `etl_sih_am.py` |
| **SINAIS_FRACOS** | Identifies sentinel diseases (e.g., Cholera, Malaria) or the second most frequent diagnosis to detect emerging outbreaks (Weak Signals). | String | Business Rule over SIH/RD / `build_gold_table.py` |
| **INTERNACOES_HIDRICAS** | Sum of admissions for water-borne diseases (ICD-10 codes A00-A09). | Float | SIH/RD (DIAG_PRINC) / `build_gold_table.py` |
| **DOENCA_HIDRICA_PREDOMINANTE** | Name of the most frequent water-borne disease (e.g., Diarrhea, Cholera). | String | SIH/RD + Unified Table (`tb_cid.txt`) / `etl_sih_am.py` |
| **TOTAL_PRODUCAO_AP** | Total volume of Primary Health Care (PHC) procedures performed. | Float | SIA/PA (QT_APROV) / `etl_sia_am.py` |
| **PROCEDIMENTO_AP_PREDOMINANTE** | Name of the most frequently performed Primary Health Care marker procedure in the unit. | String | SIA/PA + Unified Table (`tb_procedimento.txt`) / `etl_sia_am.py` |
| **STATUS_PRESSAO** | Demand pressure classification over the health system (Critical, High, Normal, Indetermined). | String | Calculated via `TAXA_OCUPACAO_ESTIMADA` / `build_gold_table.py` |
| **IS_UBS_FLUVIAL** | Boolean flag indicating whether the facility is river-based (Fluvial Basic Health Unit or Fluvial Emergency Unit). | Boolean | CNES/ST (TP_UNID 32 or 73) / `etl_cnes_am.py` |
| **CAPACIDADE_REAL_SUS** | Corrected number of public beds (Assumes all existing beds are public if management is local and SUS=0). | Float | CNES/LT (`QT_EXIST` vs `QT_SUS`) / `etl_cnes_am.py` |
| **CNES** | National Registry of Health Establishments code (Primary Key). | String | CNES / Primary key across all ETL scripts |
| **PERMANENCIA_MEDIA** | Average length of hospital stay (in days) per patient during the month. | Float | SIH (`DT_SAIDA` - `DT_INTER`) / `etl_sih_am.py` |
| **CODUFMUN** | IBGE code for the municipality (Amazonas state codes start with "13"). | String | CNES/ST and IBGE / `etl_municipios.py` |
| **TP_UNID** | Code representing the type of health unit (e.g., 32=Fluvial UBS, 05=General Hospital). | Float | CNES/ST / `etl_cnes_am.py` |
| **TPGESTAO** | Type of public management (M: Municipal, E: State, D: Dual). | String | CNES/ST / `etl_cnes_am.py` |
| **COD_CEP** | Postal code (ZIP code) of the health establishment. | Float | CNES/ST / `etl_cnes_am.py` |
| **QT_EXIST** | Total number of existing physical beds in the unit. | Float | CNES/LT / `etl_cnes_am.py` |
| **QT_SUS** | Total number of beds officially contracted for the Brazilian Unified Health System (SUS). | Float | CNES/LT / `etl_cnes_am.py` |
| **TAXA_OCUPACAO_ESTIMADA** | Estimated bed occupancy rate (Ratio between monthly admissions and installed capacity). | Float | Calculated (`TOTAL_INTERNACOES` / `CAPACIDADE_REAL_SUS`) / `build_gold_table.py` |
