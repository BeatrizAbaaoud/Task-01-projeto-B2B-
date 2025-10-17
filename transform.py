import pandas as pd
import numpy as np
import datetime

def transform_data(df: pd.DataFrame, colunas_chave: list):
    """2. TRANSFORMAÇÃO: Realiza a limpeza, filtros e modelagem dimensional."""
    print("2. TRANSFORM: Aplicando regras de negócio, filtros e modelando...")
    
    # Retorna DataFrames vazios caso a extração tenha falhado
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Selecionar e Padronizar Nomes de Colunas
    df = df[colunas_chave].copy()
    
    # Renomear colunas para o padrão do Modelo Dimensional (Tier2_PartnerName vem de 'Branch')
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

    # --- Aplicação dos Filtros do Projeto (Regras de Negócio) ---
    df = df[df['SalesType'].isin(['Sellout', 'Seltru'])].copy()
    df = df[df['Distributor'] != 'NAGEM_BRANDSHOP'].copy()
    df = df[df['Type'].isin(['MON', 'LFD', 'MOBILE'])].copy() 
    
    # Limpeza e Padronização de CNPJs
    df['Tier1_PartnerCode'] = df['Tier1_PartnerCode'].astype(str).str.replace(r'[./-]', '', regex=True).str.zfill(14)
    df['Tier2_PartnerCode'] = df['Tier2_PartnerCode'].astype(str).str.replace(r'[./-]', '', regex=True).str.zfill(14)
    df['EndCustomer_Code'] = df['EndCustomer_Code'].astype(str).str.replace(r'[./-]', '', regex=True)

    # --- Criação das Tabelas de Dimensão e Chaves ---
    
    # Dim_Data
    df_data = df[['SalesDate', 'Week']].drop_duplicates().reset_index(drop=True)
    df_data['Dia'] = df_data['SalesDate'].dt.day
    df_data['Mes'] = df_data['SalesDate'].dt.month
    df_data['Ano'] = df_data['SalesDate'].dt.year
    df_data.rename(columns={'Week': 'Semana_Fiscal'}, inplace=True)
    df_data['data_key'] = df_data.index + 1
    
    # Dim_Tipo_Venda
    df_tipo_venda = df[['SalesType']].drop_duplicates().reset_index(drop=True)
    df_tipo_venda['Descricao'] = df_tipo_venda['SalesType'].apply(
        lambda x: 'Venda Direta (Sellout)' if x == 'Sellout' else 'Venda via Parceiro (Registro de Oportunidade)'
    )
    df_tipo_venda['tipo_venda_key'] = df_tipo_venda.index + 1
    
    # Dim_Produto
    df_produto = df[['PN', 'Type', 'LFD_Flag']].drop_duplicates(subset=['PN']).reset_index(drop=True)
    df_produto['produto_key'] = df_produto.index + 1
    
    # Dim_Parceiro
    df_parceiro = df[[
        'Distributor', 'Tier1_PartnerCode', 'Tier2_PartnerCode', 
        'Tier2_PartnerName', 'Tier2_PartnerState', 'Tier2_PartnerCity', 
        'Tier2_PartnerZipCode'
    ]].drop_duplicates(subset=['Tier1_PartnerCode', 'Tier2_PartnerCode']).reset_index(drop=True)
    df_parceiro.rename(columns={
        'Distributor': 'Distributor_Name',
        'Tier2_PartnerState': 'Estado',
        'Tier2_PartnerCity': 'Cidade',
        'Tier2_PartnerZipCode': 'CEP'
    }, inplace=True)
    df_parceiro['parceiro_key'] = df_parceiro.index + 1
    
    # Dim_Cliente_Final
    df_cliente_final = df[[
        'EndCustomer_Code', 'EndCustomer_Name', 'EndCustomer_State', 
        'EndCustomer_City', 'EndCustomer_ZipCode'
    ]].drop_duplicates(subset=['EndCustomer_Code']).reset_index(drop=True)
    df_cliente_final = df_cliente_final[df_cliente_final['EndCustomer_Code'] != '0'].copy() 
    df_cliente_final['cliente_final_key'] = df_cliente_final.index + 1

    # --- Criação da Tabela Fato (Fato_Vendas_Oportunidades) ---
    df_fato = df[[
        'SalesDate', 'SalesType', 'PN', 'Tier1_PartnerCode', 
        'Tier2_PartnerCode', 'EndCustomer_Code', 'QTY', 
        'Invoice_Number', 'Projeto'
    ]].copy() 
    
    # Juntar as chaves das dimensões na tabela fato
    df_fato = pd.merge(df_fato, df_data[['SalesDate', 'data_key']], on='SalesDate', how='left')
    df_fato = pd.merge(df_fato, df_tipo_venda[['SalesType', 'tipo_venda_key']], on='SalesType', how='left')
    df_fato = pd.merge(df_fato, df_produto[['PN', 'produto_key']], on='PN', how='left')
    df_parceiro_merge = df_parceiro[['Tier1_PartnerCode', 'Tier2_PartnerCode', 'parceiro_key']]
    df_fato = pd.merge(df_fato, df_parceiro_merge, on=['Tier1_PartnerCode', 'Tier2_PartnerCode'], how='left')
    df_cliente_final_merge = df_cliente_final[['EndCustomer_Code', 'cliente_final_key']]
    df_fato = pd.merge(df_fato, df_cliente_final_merge, on='EndCustomer_Code', how='left')
    
    # Selecionar colunas finais da FATO (apenas chaves e métricas)
    df_fato = df_fato[[
        'data_key', 'tipo_venda_key', 'produto_key', 'parceiro_key', 
        'cliente_final_key', 'QTY', 'Invoice_Number', 'Projeto'
    ]].copy()
    
    df_fato['cliente_final_key'].fillna(0, inplace=True) 

    # Retorna os 6 DataFrames
    return df_fato, df_data, df_tipo_venda, df_produto, df_parceiro, df_cliente_final
