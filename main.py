import pandas as pd
import numpy as np
import datetime
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

# --- CONFIGURAÇÕES GLOBAIS ---

# CONFIGURAÇÕES DO BANCO DE DADOS (LOAD)
# Ajustado para acesso sem senha
DB_HOST = 'localhost'       
DB_USER = 'root'            
DB_PASSWORD = ''            
DB_NAME = 'projeto_b2b'     

# CONFIGURAÇÃO DE ARQUIVOS (EXTRACT/LOAD)
FILE_MON = "Base_B2B_MON_2025-W40 (1).xlsx - MON_raw.csv.csv"
FILE_MOB = "Base_B2B_NOTE_TAB_MOB_2025-W40.xlsx - Base.csv.csv"
DELIMITER = ';' 
OUTPUT_PARQUET = 'dados_b2b_processados.parquet'
OUTPUT_DIMENSAO = 'dimensoes_b2b.xlsx' 

# COLUNAS DE INTERESSE (TRANSFORM)
COLUNAS_CHAVE = [
    'Tier1_PartnerCode', 'Distributor', 'Tier2_PartnerCode', 
    'EndCustomer_Code', 'SalesDate', 'Invoice_Number', 
    'PN', 'QTY', 'SalesType', 'Type', 
    'Tier2_PartnerState', 'Tier2_PartnerCity', 
    'EndCustomer_State', 'EndCustomer_City',
    'EndCustomer_Name', 'EndCustomer_ZipCode', 
    'Branch', 
    'Tier2_PartnerZipCode',
    'Projeto', 'Week', 'LFD' 
]

def main():
    print("--- INICIANDO PIPELINE ETL B2B ---")
    
    # 1. EXTRAÇÃO
    df_consolidated = extract_data(FILE_MON, FILE_MOB, DELIMITER)
    if df_consolidated.empty:
        print("PIPELINE CANCELADO: Não foi possível extrair os dados.")
        return

    # 2. TRANSFORMAÇÃO
    df_fato, df_data, df_tipo_venda, df_produto, df_parceiro, df_cliente_final = \
        transform_data(df_consolidated, COLUNAS_CHAVE)
    
    # 3. CARGA
    load_data(df_fato, df_data, df_tipo_venda, df_produto, df_parceiro, df_cliente_final,
              DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, OUTPUT_PARQUET, OUTPUT_DIMENSAO)

    print("\nPROCESSO ETL CONCLUÍDO COM SUCESSO.")

if __name__ == '__main__':
    main()
