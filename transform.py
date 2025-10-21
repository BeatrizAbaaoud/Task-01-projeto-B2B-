# =========================================================
# 2. FUNÇÃO TRANSFORM (REESTRUTURADA PARA O FLUXO NEOTASS)
# =========================================================
def transform_data(df):
    """2. TRANSFORMAÇÃO: Realiza a limpeza, filtros e modelagem dimensional."""
    print("2. TRANSFORM: Aplicando regras de negócio, filtros e modelando...")
    
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Seleção de Colunas e Renomeação (Inicial)
    df = df[COLUNAS_CHAVE].copy()
    df.columns = [
        'Tier1_PartnerCode', 'Distributor', 'Tier2_PartnerCode', 
        'EndCustomer_Code', 'SalesDate', 'Invoice_Number', 
        'PN', 'QTY', 'SalesType', 'Type', 
        'Tier2_PartnerState', 'Tier2_PartnerCity', 
        'EndCustomer_State', 'EndCustomer_City',
        'EndCustomer_Name', 'EndCustomer_ZipCode', 
        'Tier2_PartnerName',
        'Tier2_PartnerZipCode',
        'Projeto', 'Week', 'LFD_Flag'
    ]

    # Tratamento de Tipos e Datas
    df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce').fillna(0).astype(int)
    
    def parse_date(date_val):
        try:
            if pd.api.types.is_number(date_val):
                return (datetime.datetime(1899, 12, 30) + datetime.timedelta(days=date_val)).strftime('%Y-%m-%d')
            else:
                return pd.to_datetime(date_val, format='%d/%m/%Y', errors='coerce').strftime('%Y-%m-%d')
        except:
            return pd.NaT

    df['SalesDate'] = df['SalesDate'].apply(parse_date)
    df.dropna(subset=['SalesDate'], inplace=True)
    df['SalesDate'] = pd.to_datetime(df['SalesDate'])

    # Filtros de Negócio
    df = df[df['SalesType'].isin(['Sellout', 'Seltru'])].copy()
    df = df[df['Distributor'] != 'NAGEM_BRANDSHOP'].copy()
    df = df[df['Type'].isin(['MON', 'LFD', 'MOBILE', 'TABLET', 'NOTE PC'])].copy() 
    
    # Limpeza e Padronização de CNPJs
    df['Tier1_PartnerCode'] = df['Tier1_PartnerCode'].astype(str).str.replace(r'[./-]', '', regex=True).str.zfill(14)
    df['Tier2_PartnerCode'] = df['Tier2_PartnerCode'].astype(str).str.replace(r'[./-]', '', regex=True).str.zfill(14)
    df['EndCustomer_Code'] = df['EndCustomer_Code'].astype(str).str.replace(r'[./-]', '', regex=True)
    
    # --- CRIAÇÃO DOS IDENTIFICADORES (REGRAS LEVY) ---
    
    # 1. Criar coluna CNPJ_REVENDA
    df['cnpj_revenda'] = df['Tier2_PartnerCode'].mask(df['Tier2_PartnerCode'].str.isspace() | df['Tier2_PartnerCode'].isna(), df['EndCustomer_Code'])
    df['cnpj_revenda'] = df['cnpj_revenda'].fillna(df['Tier1_PartnerCode'])
    
    # 2. NOVA CHAVE DE NEGÓCIO (fato_pk para o UPSERT)
    df['chave_negocio'] = df['Distributor'] + '_' + \
                          df['Invoice_Number'].astype(str) + '_' + \
                          df['PN'] + '_' + \
                          df['SalesDate'].dt.strftime('%Y%m%d') + '_' + \
                          df['QTY'].astype(str)

    # 3. MOCKUP DOS IDs
    df['id_revenda'] = np.nan 
    df['id_cf_raiz'] = np.nan 
    df['id_skus'] = np.nan 
    df['id_dist'] = np.nan 

    # --- CRIAÇÃO DAS TABELAS DE DIMENSÃO (Tabelas Auxiliares) ---
    
    # Ddata 
    df_ddata = df[['SalesDate', 'Week']].drop_duplicates().reset_index(drop=True)
    df_ddata['Dia'] = df_ddata['SalesDate'].dt.day
    df_ddata['Mes'] = df_ddata['SalesDate'].dt.month
    df_ddata['Ano'] = df_ddata['SalesDate'].dt.year
    df_ddata.rename(columns={'Week': 'Semana_Fiscal'}, inplace=True)
    df_ddata['data_key'] = df_ddata.index + 1
    
    # Dtipovenda
    df_dtipovenda = df[['SalesType']].drop_duplicates().reset_index(drop=True)
    df_dtipovenda['tipo_venda_key'] = df_dtipovenda.index + 1

    # Dproduto
    df_dproduto = df[['PN', 'Type', 'LFD_Flag']].drop_duplicates(subset=['PN']).reset_index(drop=True)
    df_dproduto['produto_key'] = df_dproduto.index + 1
    
    # Drevenda (Consolida Parceiro e Cliente Final)
    df_drevenda = df[['cnpj_revenda', 'Distributor', 'Tier1_PartnerCode', 'Tier2_PartnerName', 'Tier2_PartnerState', 'Tier2_PartnerCity']].copy()
    
    # RENOMEAR COLUNAS PARA MINÚSCULO (O MAIS SEGURO PARA O MYSQL)
    df_drevenda.rename(columns={
        'Distributor': 'distributor_name', 
        'Tier1_PartnerCode': 'tier1_partner_code',
        'Tier2_PartnerName': 'tier2_partner_name',
        'Tier2_PartnerState': 'estado',
        'Tier2_PartnerCity': 'cidade'
    }, inplace=True)
    
    df_drevenda['Natureza'] = 'Venda/Revenda Raiz' 
    df_drevenda = df_drevenda.drop_duplicates(subset=['cnpj_revenda']).reset_index(drop=True)
    df_drevenda['drevenda_key'] = df_drevenda.index + 1

    # Ddistribuidor
    df_ddistribuidor = df[['Distributor', 'Tier1_PartnerCode']].drop_duplicates().reset_index(drop=True)
    df_ddistribuidor.rename(columns={'Distributor': 'nome_distribuidor', 'Tier1_PartnerCode': 'cnpj_distribuidor'}, inplace=True)
    df_ddistribuidor['ddist_key'] = df_ddistribuidor.index + 1

    # --- CRIAÇÃO DA TABELA FATO (FSellout) ---
    df_fsellout = df[[
        'id_revenda', 'id_cf_raiz', 'id_skus', 'id_dist',
        'chave_negocio', 'SalesDate', 'SalesType', 'PN', 'QTY', 'Invoice_Number', 'Projeto',
        'cnpj_revenda', 'Distributor' 
    ]].copy() 
    
    # Merge das Dimensões Chave
    df_fsellout = pd.merge(df_fsellout, df_ddata[['SalesDate', 'data_key']], on='SalesDate', how='left')
    df_fsellout = pd.merge(df_fsellout, df_dtipovenda[['SalesType', 'tipo_venda_key']], on='SalesType', how='left')
    df_fsellout = pd.merge(df_fsellout, df_dproduto[['PN', 'produto_key']], on='PN', how='left')

    # Seleção final de colunas para FATO
    df_fsellout = df_fsellout[[
        'chave_negocio', 'data_key', 'tipo_venda_key', 'produto_key',
        'id_revenda', 'id_cf_raiz', 'id_skus', 'id_dist', 
        'QTY', 'Invoice_Number', 'Projeto'
    ]].copy()
    
    df_fsellout[['id_revenda', 'id_cf_raiz', 'id_skus', 'id_dist']] = df_fsellout[['id_revenda', 'id_cf_raiz', 'id_skus', 'id_dist']].fillna(0).astype(int)
    
    return df_fsellout, df_ddata, df_dtipovenda, df_dproduto, df_drevenda, df_ddistribuidor
