import pandas as pd
import numpy as np

def extract_data(file_mon: str, file_mob: str, delimiter: str):
    """1. EXTRAÇÃO: Extrai e consolida os dados das duas fontes CSV."""
    print("1. EXTRACT: Extraindo e consolidando dados...")
    try:
        df_mon = pd.read_csv(file_mon, delimiter=delimiter)
        df_mob = pd.read_csv(file_mob, delimiter=delimiter)
        
        # Garante que a coluna 'LFD' exista em ambas as bases antes de unir
        if 'LFD' not in df_mob.columns:
            df_mob['LFD'] = np.nan
        
        df_consolidated = pd.concat([df_mon, df_mob], ignore_index=True)
        return df_consolidated
    except FileNotFoundError as e:
        print(f"ERRO: Arquivo não encontrado. Verifique se o nome está correto: {e}")
        return pd.DataFrame()
