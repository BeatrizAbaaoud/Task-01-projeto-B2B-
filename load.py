import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

def load_data(df_fato, df_data, df_tipo_venda, df_produto, df_parceiro, df_cliente_final,
              db_host, db_user, db_password, db_name, output_parquet, output_dimensao):
    """3. CARGA: Carrega os DataFrames diretamente nas tabelas MySQL e gera backups."""
    print("3. LOAD: Carregando dados diretamente para o MySQL...")

    # --- CARGA MYSQL ---
    try:
        engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

        # 1. Carregar Dimensões 
        df_data.to_sql('Dim_Data', con=engine, if_exists='append', index=False)
        print(" -> Dim_Data carregada.")
        df_tipo_venda.to_sql('Dim_Tipo_Venda', con=engine, if_exists='append', index=False)
        print(" -> Dim_Tipo_Venda carregada.")
        df_produto.to_sql('Dim_Produto', con=engine, if_exists='append', index=False)
        print(" -> Dim_Produto carregada.")
        df_parceiro.to_sql('Dim_Parceiro', con=engine, if_exists='append', index=False)
        print(" -> Dim_Parceiro carregada.")
        df_cliente_final.to_sql('Dim_Cliente_Final', con=engine, if_exists='append', index=False)
        print(" -> Dim_Cliente_Final carregada.")

        # 2. Carregar a Tabela Fato
        df_fato.to_sql('Fato_Vendas_Oportunidades', con=engine, if_exists='append', index=False)
        print(" -> Fato_Vendas_Oportunidades carregada.")
        
        engine.dispose()
        print(" -> Conexão MySQL finalizada com sucesso.")
        
    except Exception as e:
        print(f"ERRO CRÍTICO NA CONEXÃO OU CARGA MYSQL: {e}")
        print("⚠️ Verifique se o servidor MySQL está rodando e se o DB_USER e DB_PASSWORD estão corretos.")
        
    # --- Geração de Backups (Parquet e Excel) ---
    df_fato.to_parquet(output_parquet, index=False)
    print(f"FATO de Vendas/Oportunidades carregada em: {output_parquet} (Backup)")
    
    with pd.ExcelWriter(output_dimensao) as writer:
        df_data.to_excel(writer, sheet_name='Dim_Data', index=False)
        df_tipo_venda.to_excel(writer, sheet_name='Dim_Tipo_Venda', index=False)
        df_produto.to_excel(writer, sheet_name='Dim_Produto', index=False)
        df_parceiro.to_excel(writer, sheet_name='Dim_Parceiro', index=False)
        df_cliente_final.to_excel(writer, sheet_name='Dim_Cliente_Final', index=False)
    print(f"Dimensões carregadas em: {output_dimensao} (Backup)")
