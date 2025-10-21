# =========================================================
# 1. FUNÇÃO EXTRACT
# =========================================================
def extract_data() -> pd.DataFrame:
    """1. EXTRAÇÃO: Extrai e consolida os dados das duas fontes CSV."""
    print("1. EXTRACT: Extraindo e consolidando dados...")
    try:
        df_mon = pd.read_csv(FILE_MON, delimiter=DELIMITER)
        df_mob = pd.read_csv(FILE_MOB, delimiter=DELIMITER)
        if 'LFD' not in df_mob.columns:
            df_mob['LFD'] = np.nan
        df_consolidated = pd.concat([df_mon, df_mob], ignore_index=True)
        return df_consolidated
    except FileNotFoundError as e:
        print(f"ERRO: Arquivo não encontrado. Verifique o nome: {e}")
        return pd.DataFrame()
