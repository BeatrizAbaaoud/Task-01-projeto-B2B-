drevend-- Use este script no MySQL para criar as tabelas.

-- 1. Dimensão de Data (Dim_Data)
CREATE TABLE Dim_Data (
    data_key INT PRIMARY KEY AUTO_INCREMENT,
    SalesDate DATE NOT NULL,
    Dia INT,
    Mes INT,
    Ano INT,
    Semana_Fiscal VARCHAR(10)
);

-- 2. Dimensão de Tipo de Venda (Dim_Tipo_Venda)
CREATE TABLE Dim_Tipo_Venda (
    tipo_venda_key INT PRIMARY KEY AUTO_INCREMENT,
    SalesType VARCHAR(50) NOT NULL UNIQUE,
    Descricao VARCHAR(100)
);

-- 3. Dimensão de Produto (Dim_Produto)
CREATE TABLE Dim_Produto (
    produto_key INT PRIMARY KEY AUTO_INCREMENT,
    PN VARCHAR(100) NOT NULL UNIQUE,
    Type VARCHAR(50), 
    LFD_Flag VARCHAR(10) 
);

-- 4. Dimensão de Parceiro (Dim_Parceiro)
CREATE TABLE Dim_Parceiro (
    parceiro_key INT PRIMARY KEY AUTO_INCREMENT,
    Distributor_Name VARCHAR(100) NOT NULL,
    Tier1_PartnerCode VARCHAR(18), -- CNPJ Principal
    Tier2_PartnerCode VARCHAR(18), -- CNPJ do Parceiro
    Tier2_PartnerName VARCHAR(100),
    Estado VARCHAR(5)
    -- ... mais colunas de localização e contato
);

-- 5. Dimensão de Cliente Final (Dim_Cliente_Final)
CREATE TABLE Dim_Cliente_Final (
    cliente_final_key INT PRIMARY KEY AUTO_INCREMENT,
    EndCustomer_Code VARCHAR(50) NOT NULL UNIQUE, 
    EndCustomer_Name VARCHAR(255)
    -- ... mais colunas
);

-- 6. Tabela Fato (Fato_Vendas_Oportunidades)
CREATE TABLE Fato_Vendas_Oportunidades (
    fato_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Chaves Estrangeiras (Foreign Keys)
    data_key INT NOT NULL,
    tipo_venda_key INT NOT NULL,
    produto_key INT NOT NULL,
    parceiro_key INT NOT NULL,
    cliente_final_key INT,
    
    -- Métrica
    QTY INT NOT NULL,
    
    -- Relações (Constraints - A ser definido após a criação das Dimensões no BD)
    FOREIGN KEY (data_key) REFERENCES Dim_Data(data_key)
    -- ... outras chaves estrangeiras
);a_raiz
