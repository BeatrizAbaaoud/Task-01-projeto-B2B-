-- Use este script no MySQL para criar a estrutura do Modelo Dimensional
-- que é alimentado pelo pipeline Python (etl_b2b_vendas.py.py).
-- Todas as tabelas estão em minúsculas para garantir a compatibilidade com o MySQL.

-- -----------------------------------------------------------
-- DIMENSÕES
-- -----------------------------------------------------------

-- 1. Dimensão de Data (dim_data)
CREATE TABLE dim_data (
    data_key INT PRIMARY KEY AUTO_INCREMENT,
    SalesDate DATE NOT NULL UNIQUE,
    Dia INT,
    Mes INT,
    Ano INT,
    Semana_Fiscal VARCHAR(10)
);

-- 2. Dimensão de Tipo de Venda (dim_tipo_venda)
CREATE TABLE dim_tipo_venda (
    tipo_venda_key INT PRIMARY KEY AUTO_INCREMENT,
    SalesType VARCHAR(50) NOT NULL UNIQUE,
    Descricao VARCHAR(100)
);

-- 3. Dimensão de Produto (dim_produto)
CREATE TABLE dim_produto (
    produto_key INT PRIMARY KEY AUTO_INCREMENT,
    PN VARCHAR(100) NOT NULL UNIQUE,
    Type VARCHAR(50), 
    LFD_Flag VARCHAR(10) 
);

-- 4. Dimensão de Revenda/Parceiro/Cliente Final (drevenda)
-- Esta dimensão consolida o CNPJ da Revenda (regra de negócio Levy)
CREATE TABLE drevenda (
    drevenda_key INT PRIMARY KEY AUTO_INCREMENT,
    cnpj_revenda VARCHAR(18) NOT NULL UNIQUE, -- Chave de consolidação
    distributor_name VARCHAR(100),
    tier1_partner_code VARCHAR(18),
    tier2_partner_name VARCHAR(100),
    estado VARCHAR(5),
    cidade VARCHAR(100),
    Natureza VARCHAR(50) -- Ex: 'Venda/Revenda Raiz'
);

-- 5. Dimensão de Distribuidor (ddistribuidor)
-- Contém a lista de Distribuidores e seus CNPJs Tier 1
CREATE TABLE ddistribuidor (
    ddist_key INT PRIMARY KEY AUTO_INCREMENT,
    nome_distribuidor VARCHAR(100) NOT NULL UNIQUE,
    cnpj_distribuidor VARCHAR(18)
);

-- -----------------------------------------------------------
-- TABELA FATO (fsellout)
-- -----------------------------------------------------------

CREATE TABLE fsellout (
    -- Chave Primária Artificial
    fato_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Chave de Negócio (Usada para o UPSERT no Python)
    chave_negocio VARCHAR(255) NOT NULL UNIQUE, 

    -- Chaves Estrangeiras do Modelo Estrela
    data_key INT NOT NULL,
    tipo_venda_key INT NOT NULL,
    produto_key INT NOT NULL,

    -- Chaves Placeholder para o futuro (Enriquecimento/Fase 2)
    id_revenda INT,
    id_cf_raiz INT,
    id_skus INT,
    id_dist INT,
    
    -- Métricas e Atributos de Degeneração
    QTY INT NOT NULL,
    Invoice_Number VARCHAR(50), 
    Projeto VARCHAR(50),
    
    -- Relações (Constraints)
    FOREIGN KEY (data_key) REFERENCES dim_data(data_key),
    FOREIGN KEY (tipo_venda_key) REFERENCES dim_tipo_venda(tipo_venda_key),
    FOREIGN KEY (produto_key) REFERENCES dim_produto(produto_key)
    -- As chaves Placeholder não recebem FOREIGN KEY nesta fase inicial.
);
