 =========================================================
# 3. FUNÇÃO LOAD (COM LÓGICA UPSERT) - CORREÇÃO DE FORMATO FINAL
# =========================================================
def load_data(df_fsellout, df_ddata, df_dtipovenda, df_dproduto, df_drevenda, df_ddistribuidor):
    """3. CARGA: Carrega os DataFrames diretamente nas tabelas MySQL."""
    print("3. LOAD: Carregando dados diretamente para o MySQL...")

    try:
        engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

        # 1. Carregar Dimensões (USANDO NOMES MINÚSCULOS)
        df_ddata.to_sql('dim_data', con=engine, if_exists='append', index=False)
        print(" -> dim_data carregada.")
        df_dtipovenda.to_sql('dim_tipo_venda', con=engine, if_exists='append', index=False)
        print(" -> dim_tipo_venda carregada.")
        df_dproduto.to_sql('dim_produto', con=engine, if_exists='append', index=False)
        print(" -> dim_produto carregada.")
        
        df_drevenda.to_sql('drevenda', con=engine, if_exists='append', index=False)
        print(" -> drevenda (Parceiros/Clientes) carregada.")
        df_ddistribuidor.to_sql('ddistribuidor', con=engine, if_exists='append', index=False)
        print(" -> ddistribuidor carregada.")


        # 2. Carregar a Tabela Fato (UPSERT)
        STAGING_TABLE = 'fato_fsellout_staging' # Corrigido para minúsculo
        TARGET_TABLE = 'fsellout' 

        df_fsellout.to_sql(STAGING_TABLE, con=engine, if_exists='replace', index=False)
        print(f" -> Tabela Staging '{STAGING_TABLE}' criada para atualização.")

        # Comando SQL que faz o UPSERT
        update_cols = [col for col in df_fsellout.columns if col != 'chave_negocio']
        update_set_sql = ', '.join([f"{col} = VALUES({col})" for col in update_cols])

        update_query = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(df_fsellout.columns)})
        SELECT {', '.join(df_fsellout.columns)} FROM {STAGING_TABLE}
        ON DUPLICATE KEY UPDATE
            {update_set_sql}
        """
        
        # O AJUSTE FINAL CRÍTICO: Limpa a string de caracteres invisíveis
        final_query = ' '.join(update_query.split()) # Remove quebras de linha e espaços duplos

        with engine.begin() as connection:
            connection.execute(text(final_query)) # <<< AQUI O TEXT() RESOLVE O PROBLEMA DE 'NOT AN EXECUTABLE OBJECT'
        
        print(f" -> {TARGET_TABLE} ATUALIZADA (UPSERT) com sucesso.")
        
        # Limpeza da tabela temporária
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE {STAGING_TABLE}")) # Usando text() para a limpeza também
        print(" -> Limpeza da tabela Staging concluída.")
        
        engine.dispose()
        print(" -> Conexão MySQL finalizada com sucesso.")
        
    except Exception as e:
        print(f"ERRO CRÍTICO NA CONEXÃO OU CARGA MYSQL: {e}")
        print("⚠️ Verifique se o servidor MySQL está rodando, se o DB_USER e DB_PASSWORD estão corretos e se a chave UNIQUE KEY está na Fato.")
        
    # Geração de Backups (Restante do código de backup...)
    df_fsellout.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"FATO de Vendas/Oportunidades carregada em: {OUTPUT_PARQUET} (Backup)")
    
    with pd.ExcelWriter(OUTPUT_DIMENSAO) as writer:
        df_ddata.to_excel(writer, sheet_name='Ddata', index=False)
        df_dtipovenda.to_excel(writer, sheet_name='Dtipovenda', index=False)
        df_dproduto.to_excel(writer, sheet_name='Dproduto', index=False)
        df_drevenda.to_excel(writer, sheet_name='Drevenda', index=False)
        df_ddistribuidor.to_excel(writer, sheet_name='Ddistribuidor', index=False)
    print(f"Dimensões carregadas em: {OUTPUT_DIMENSAO} (Backup)")
