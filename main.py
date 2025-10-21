import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine, text # IMPORTANTE: Adicionado 'text'
import mysql.connector
from typing import Tuple, List

# --- CONFIGURAÇÕES DO BANCO DE DADOS (PRODUÇÃO) ---
DB_HOST = 'neotasssp1.mysql.database.azure.com'
DB_USER = 'beatriz.abaaoud'
DB_PASSWORD = 'beatriz_abaaoud2025'
DB_NAME = 'bd_smvp' # SCHEMA DE PRODUÇÃO
# -------------------------------------------------------------------

# --- CONFIGURAÇÃO DE ARQUIVOS ---
FILE_MON = "Base_B2B_MON_2025-W40 (1).xlsx - MON_raw.csv.csv"
FILE_MOB = "Base_B2B_NOTE_TAB_MOB_2025-W40.xlsx - Base.csv.csv"
DELIMITER = ';' 
OUTPUT_PARQUET = 'dados_b2b_processados_final.parquet'
OUTPUT_DIMENSAO = 'dimensoes_b2b_final.xlsx' 

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
