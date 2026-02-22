import pandas as pd
import json
import uuid
import logging
import os
import sys
from typing import Dict, Any, List

# Ensure project root is in sys.path for direct execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src import config

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def define_analysis_focus(row: pd.Series) -> Dict[str, str]:
    """
    Decides if the focus is Clinical/Hospital or Prevention/Primary Care.
    
    Args:
        row (pd.Series): Row of the dataframe containing facility data.

    Returns:
        Dict[str, str]: Dictionary containing 'focus_type' and 'task_text'.
    """
    total_admissions = float(row.get('TOTAL_INTERNACOES', 0))
    pred_diag = row.get('DIAGNOSTICO_PREDOMINANTE', 'N/A')
    
    if total_admissions > 0 and pred_diag != 'N/A':
        return {
            "focus_type": "CLÍNICO-HOSPITALAR (Demanda Aguda)",
            "task_text": (
                f"Analise o quadro de internações por {pred_diag}. "
                f"Avalie como a estação {row['ESTACAO_AMAZONICA']} impacta especificamente a realização deste manejo clínico e a necessidade de leitos."
            )
        }
    else:
        proc_pred = row.get('PROCEDIMENTO_AP_PREDOMINANTE', 'Atendimentos Básicos')
        return {
            "focus_type": "PREVENÇÃO E ATENÇÃO PRIMÁRIA (APS)",
            "task_text": (
                f"A unidade focou em '{proc_pred}'. "
                f"Avalie como a estação {row['ESTACAO_AMAZONICA']} impacta especificamente a realização deste procedimento e se essa produção é estratégica para mitigar os riscos sazonais."
            )
        }


def inject_seasonal_intelligence(row: pd.Series) -> str:
    """
    Explicit correlation between Seasonality and Pathology.
    
    Args:
        row (pd.Series): Row of the dataframe.

    Returns:
        str: Specialist note regarding seasonality context.
    """
    season = str(row.get('ESTACAO_AMAZONICA', 'Indeterminado'))
    water_disease = str(row.get('DOENCA_HIDRICA_PREDOMINANTE', '')).lower()
    water_admissions = float(row.get('INTERNACOES_HIDRICAS', 0))
    
    specialist_note = ""
    
    if season == 'Vazante':  # Low Water Season
        base_note = " Rios em nível baixo. Acesso difícil. Risco de água parada e poços contaminados."
        if water_admissions > 0 or 'diarréia' in water_disease or 'gastroenterite' in water_disease or 'cólera' in water_disease:
            specialist_note = (
                f"ALERTA AMBIENTAL: {base_note} A detecção de {water_disease} na Vazante sugere "
                "contaminação de fontes locais. RECOMENDAÇÃO: Distribuição imediata de hipoclorito."
            )
        else:
            specialist_note = f"CONTEXTO VAZANTE: {base_note}"
    else:  # High Water Season (Cheia)
        base_note = " Rios em nível extremo. Comunidades alagadas. Atracadouros submersos impedindo a atracação da UBS Fluvial."
        specialist_note = f"CONTEXTO CHEIA: {base_note}"
        
    return specialist_note


def interpret_pressure(row: pd.Series) -> str:
    """
    Handles Indetermined Status and Fluvial Specificity.
    
    Args:
        row (pd.Series): Row of the dataframe.

    Returns:
        str: Interpretative text about system pressure/capacity.
    """
    status = row.get('STATUS_PRESSAO', 'Indeterminado')
    is_fluvial = row.get('IS_UBS_FLUVIAL', 0) == 1
    
    if is_fluvial:
        return (
            "ESTRATÉGIA FLUVIAL: Esta é uma Unidade Móvel. O fator crítico não é apenas a ocupação, "
            "mas o tempo de permanência na comunidade e a autonomia de insumos durante a navegação."
        )
    elif status == 'Indeterminado':
        return "CAPACIDADE: Dados de ocupação inconclusivos ou unidade sem leitos de internação. Focar em resolutividade ambulatorial."
    else:
        return f"CAPACIDADE: O sistema apresenta pressão classificada como {status}."


def treat_demand_profile(row: pd.Series) -> str:
    """
    Replaces null diagnoses with functional PHC context.
    
    Args:
        row (pd.Series): Row of the dataframe.

    Returns:
        str: Diagnosis or context string.
    """
    diag = str(row.get('DIAGNOSTICO_PREDOMINANTE', 'N/A'))
    focus = row.get('PROCEDIMENTO_AP_PREDOMINANTE', 'Atendimentos Gerais')
    
    # Check for various forms of null/empty
    if diag.lower() in ['nan', 'n/a', 'none', ''] or pd.isna(row.get('DIAGNOSTICO_PREDOMINANTE')):
        return f"Não aplicável (Foco em Rastreio e Prevenção: {focus})"
    
    return diag


def detect_sentinel_event(hist_df: pd.DataFrame, current_row: pd.Series) -> str:
    """
    Analyzes if there was a production or hospitalization peak in the last months.
    
    Args:
        hist_df (pd.DataFrame): Historical data for the unit.
        current_row (pd.Series): Current month data.

    Returns:
        str: Alert message if anomalies are detected.
    """
    if hist_df.empty:
        return ""
        
    alerts = []
    
    # 1. Analyze Ambulatory Production Peaks (Campaigns)
    avg_prod = hist_df['TOTAL_PRODUCAO_AP'].mean()
    if avg_prod > 0:
        # Filter: 1.5x average AND minimum absolute volume of 50 to be considered "campaign"
        prod_peaks = hist_df[(hist_df['TOTAL_PRODUCAO_AP'] > (avg_prod * 1.5)) & (hist_df['TOTAL_PRODUCAO_AP'] > 50)]
        if not prod_peaks.empty:
            p = prod_peaks.iloc[-1]
            alerts.append(f"PRODUÇÃO PICO: Em {p['COMPETENCIA']} houve um volume de {int(p['TOTAL_PRODUCAO_AP'])} atendimentos ({p['PROCEDIMENTO_AP_PREDOMINANTE']}).")

    # 2. Analyze Hospitalization Peaks (Outbreaks)
    avg_adm = hist_df['TOTAL_INTERNACOES'].mean()
    if avg_adm > 0:
        # Filter: 1.5x average AND minimum volume of 5 extra admissions to avoid noise
        adm_peaks = hist_df[(hist_df['TOTAL_INTERNACOES'] > (avg_adm * 1.5)) & (hist_df['TOTAL_INTERNACOES'] > 5)]
        if not adm_peaks.empty:
            p = adm_peaks.iloc[-1]
            alerts.append(f"PICO DE ADMISSÕES: Em {p['COMPETENCIA']} houve um surto/aumento de {int(p['TOTAL_INTERNACOES'])} casos ({p['DIAGNOSTICO_PREDOMINANTE']}).")
    
    # 3. Analyze Abrupt Production Drops (Access/Supply Rupture)
    current_prod = float(current_row.get('TOTAL_PRODUCAO_AP', 0))
    if avg_prod > 50 and current_prod < (avg_prod * 0.5):
        alerts.append(f"QUEDA REPENTINA NA PRODUÇÃO: Volume atual ({int(current_prod)}) é inferior a 50% da média histórica ({int(avg_prod)}). Investigar bloqueios de acesso devido à seca severa, avaria de embarcações ou falta de suprimentos.")

    if alerts:
        msg = "⚠️ **ALERTA DE VIGILÂNCIA:** " + " | ".join(alerts)
        msg += " O protocolo exige investigação imediata das causas e um plano de contingência para garantir a continuidade do atendimento."
        return msg
    
    return "Não foi detectada nenhuma variação estatística relevante no histórico de 90 dias."


def generate_prompt(row: pd.Series, history: str = "Not available", sentinel_intelligence: str = "") -> str:
    """
    Final prompt assembly following Markdown template with Clinical Memory, Computational Intelligence, and Structured Output.
    
    Args:
        row (pd.Series): Current data row.
        history (str): Textual history of previous months.
        sentinel_intelligence (str): Anomalies detected.

    Returns:
        str: Formatted prompt string.
    """
    focus_context = define_analysis_focus(row)
    seasonal_tip = inject_seasonal_intelligence(row)
    capacity_context = interpret_pressure(row)
    demand_profile = treat_demand_profile(row)
    
    prompt = f"""# PAPEL
Atue como um Médico Sanitarista especialista em Saúde Pública na Amazônia.

# CENÁRIO SITUACIONAL
- **Local:** {row['NO_MUNICIPIO']} (Unidade: {row['NO_ESTABELECIMENTO']})
- **Período:** {row['COMPETENCIA']} - Estação: **{row['ESTACAO_AMAZONICA']}**

# MEMÓRIA CLÍNICA (Contexto Longitudinal - Últimos 90 dias)
{history}

# INTELIGÊNCIA COMPUTACIONAL (Detecção de Anomalias)
{sentinel_intelligence}

# DADOS RELEVANTES (Mês Atual)
- **Foco da Análise:** {focus_context['focus_type']}
- **Perfil da Demanda / Diagnóstico:** {demand_profile}
- **Sinais Fracos / Alertas:** {row.get('SINAIS_FRACOS', 'Estável')}
- **Doença Hídrica Predominante:** {row.get('DOENCA_HIDRICA_PREDOMINANTE', 'None')}
- **Procedimento APS mais realizado:** {row.get('PROCEDIMENTO_AP_PREDOMINANTE', 'N/A')}

# ANÁLISE DE CAPACIDADE E LOGÍSTICA
{capacity_context}

# INTELIGÊNCIA EPIDEMIOLÓGICA
{seasonal_tip}

# TAREFA
{focus_context['task_text']}
1. Identifique tendências comparando o cenário atual com o histórico recente.
2. Recomende 3 ações prioritárias para os próximos 30 dias (considere o Alerta de Rastreamento se presente).
3. Determine se a conduta deve ser tratamento local ou remoção para centro de referência.

# FORMATO DE SAÍDA OBRIGATÓRIO (JSON)
Você deve responder EXCLUSIVAMENTE com um objeto JSON válido seguindo esta estrutura:
{{
  "analise_situacional": "Seu raciocínio clínico (Chain of Thought). Explique POR QUE chegou às recomendações, citando a sazonalidade e desvios históricos.",
  "recomendacoes_prioritarias": [
    "Ação 1",
    "Ação 2",
    "Ação 3"
  ],
  "nivel_alerta": "Baixo | Médio | Alto",
  "estrategia_logistica": "Tratamento Local | Remoção | Itinerância"
}}

### INSTRUÇÕES CRÍTICAS PARA 'nivel_alerta':
- O campo 'nivel_alerta' refere-se à **urgência da intervenção do gestor** (necessidade de suporte, recursos ou investigação) e não meramente ao volume de atendimentos.
- Se a seção 'INTELIGÊNCIA COMPUTACIONAL' indicar 'QUEDA ABRUPTA', 'PICO DE PRODUÇÃO' ou 'PICO DE INTERNAÇÃO', o 'nivel_alerta' DEVE ser classificado como **Médio** ou **Alto**.

Não inclua texto explicativo fora do objeto JSON.
"""
    return prompt.strip()


def main() -> None:
    input_csv = config.OUTPUT_FILE_GOLD
    output_jsonl = config.OUTPUT_FILE_PROMPTS
    
    if not os.path.exists(input_csv):
        logging.error(f"File {input_csv} not found.")
        return

    logging.info(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv)
    
    # Sort to ensure correct sliding window
    df = df.sort_values(['CNES', 'COMPETENCIA'])
    
    artifacts: List[Dict[str, Any]] = []
    logging.info("Generating prompts enriched with Clinical Memory...")
    
    # Group by CNES to process history of each unit
    for cnes, group in df.groupby('CNES'):
        group = group.reset_index(drop=True)
        for i, row in group.iterrows():
            if pd.isna(row['NO_MUNICIPIO']) or pd.isna(row['NO_ESTABELECIMENTO']):
                continue
            
            # Capture history (up to 2 months prior)
            hist_rows = group.iloc[max(0, i-2):i]
            if not hist_rows.empty:
                hist_text = ""
                for _, h_row in hist_rows.iterrows():
                    hist_text += f"- {h_row['COMPETENCIA']} ({h_row['ESTACAO_AMAZONICA']}): {int(h_row['TOTAL_INTERNACOES'])} adm., Diag: {h_row['DIAGNOSTICO_PREDOMINANTE']}, Alerta: {h_row['SINAIS_FRACOS']}\n"
                history = hist_text.strip()
                # Peak Detection Intelligence
                sentinel_intelligence = detect_sentinel_event(hist_rows, row)
            else:
                history = "Primeiro registro desta unidade no período de monitoramento."
                sentinel_intelligence = "Histórico insuficiente para análise estatística."
            
            prompt_text = generate_prompt(row, history, sentinel_intelligence)
            
            artifact = {
                "id": str(uuid.uuid4()),
                "meta": {
                    "cnes": str(row['CNES']),
                    "municipality": row['NO_MUNICIPIO'],
                    "competence": row['COMPETENCIA'],
                    "season": row['ESTACAO_AMAZONICA'],
                    "is_fluvial": bool(row.get('IS_UBS_FLUVIAL', 0))
                },
                "prompt": prompt_text,
                "config": {
                    "model": "google/medgemma-4b-it",
                    "temperature": 0.2,
                    "max_output_tokens": 1024
                }
            }
            artifacts.append(artifact)
            
    logging.info(f"Saving {len(artifacts)} artifacts to {output_jsonl}...")
    with open(output_jsonl, 'w', encoding='utf-8') as f:
        for item in artifacts:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
    logging.info("Process completed successfully.")


if __name__ == "__main__":
    main()
