------------------------------------------------------------------------------------------------------------
------------------------------------------------	BEGIN	------------------------------------------------
------------------------------------------------------------------------------------------------------------
/*
	O script abaixo cria e e popula os dados nas estruturas de tabelas criadas a fim de possibilitar a execução das consultas do questionário
*/

-- Criando o banco de dados Concentrix
CREATE DATABASE Concentrix;
GO

-- Usar o banco de dados criado
USE Concentrix;
GO

---- Criação da tabela de clientes
DROP TABLE IF EXISTS tb_customers
CREATE TABLE tb_customers (
	 customerId INT IDENTITY(1,1) PRIMARY KEY
	,customerDoc CHAR(14)
	,firstName VARCHAR(32)
	,lastName VARCHAR(32)
	,birthDate DATE
)

-- Criação da tabela de produtos
DROP TABLE IF EXISTS tb_products;
CREATE TABLE tb_products (
    productId INT IDENTITY(1,1) PRIMARY KEY,
    productName VARCHAR(50),
    price DECIMAL(10, 2)
);

-- Criação da tabela de pedidos
DROP TABLE IF EXISTS tb_orders;
CREATE TABLE tb_orders (
    orderId INT IDENTITY(1,1) PRIMARY KEY,
    customerId INT FOREIGN KEY REFERENCES tb_customers(customerId),
    orderDate DATE
);

-- Criação da tabela de itens de pedidos
DROP TABLE IF EXISTS tb_order_items;
CREATE TABLE tb_order_items (
    orderItemId INT IDENTITY(1,1) PRIMARY KEY,
    orderId INT FOREIGN KEY REFERENCES tb_orders(orderId),
    productId INT FOREIGN KEY REFERENCES tb_products(productId),
    quantity INT
);

-- Índices para otimização de consultas
CREATE NONCLUSTERED INDEX ix_customerDoc on tb_customers (customerDoc) INCLUDE (customerId)
CREATE NONCLUSTERED INDEX ix_customerId ON tb_orders (customerId);
CREATE NONCLUSTERED INDEX ix_orderId ON tb_order_items (orderId);
CREATE NONCLUSTERED INDEX ix_productId ON tb_order_items (productId);

-- População de dados na tabela de tb_customers
INSERT INTO tb_customers (customerDoc, firstName, lastName, birthDate)
SELECT
    RIGHT('00000000000' + CAST(ABS(CHECKSUM(NEWID())) % 100000000000000 AS VARCHAR), 11) AS customerDoc,
    LEFT(NEWID(), 32) AS firstName,
    LEFT(NEWID(), 32) AS lastName,
    DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 36525, '1906-01-01') AS birthDate
FROM
    (SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS r FROM sys.columns) AS Numbers;

-- População de dados na tabela de produtos
INSERT INTO tb_products (productName, price)
VALUES
    ('Notebook', 2100.00),
    ('Smartphone', 1200.00),
    ('Tablet', 800.00),
    ('Headphones', 150.00),
    ('Monitor', 500.00),
    ('Keyboard', 80.00),
    ('Mouse', 40.00),
    ('Printer', 250.00),
    ('Webcam', 100.00),
    ('Speakers', 75.00),
    ('External Hard Drive', 130.00),
    ('USB Flash Drive', 20.00),
    ('Router', 90.00),
    ('Smartwatch', 250.00),
    ('Fitness Tracker', 100.00);

-- População de dados na tabela de pedidos
DECLARE @COUNT INT = 0, @LIM INT = 5
WHILE @COUNT <= @LIM BEGIN
	INSERT INTO tb_orders (customerId, orderDate)
	SELECT
		customerId,
		DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 365, GETDATE()) AS orderDate
	FROM
		tb_customers
	WHERE
		customerId <= (SELECT ABS(CHECKSUM(NEWID())) % 1000)

	SET @COUNT += 1

END

-- População de dados na tabela de itens de pedidos
DECLARE @COUNT1 INT = 0, @LIM1 INT = 10
WHILE @COUNT1 <= @LIM1 BEGIN
INSERT INTO tb_order_items (orderId, productId, quantity)
SELECT
    o.orderId,
    p.productId,
    ABS(CHECKSUM(NEWID())) % 5 + 1 AS quantity 
FROM
    tb_orders o
    CROSS JOIN tb_products p
WHERE
    ABS(CHECKSUM(NEWID())) % 10 < 3; 

	SET @COUNT1 += 1
END

------------------------------------------------------------------------------------------------------------
------------------------------------------------	END		------------------------------------------------
------------------------------------------------------------------------------------------------------------


-- RESOLVENDO AS QUESTÕES DE NEGÓCIO

-- 1.	Crie uma consulta que retorne apenas o item mais pedido e a quantidade total de pedidos.

SELECT TOP 1
	p.productId,
	p.productName,
	SUM(oi.quantity) AS totalQuantityOrdered
FROM
    [master].[dbo].[tb_order_items] oi
JOIN
    [master].[dbo].[tb_products] p ON oi.productId = p.productId
GROUP BY
	p.productId, p.productName
ORDER BY
	totalQuantityOrdered DESC;

-- Explicação da Consulta
-- JOIN junta a tabela tb_order_items e tb_products usando a coluna productId.
-- SUM calcula a quantidade total de cada produto pedido, agrupando por productId e productName.
-- ORDER BY ordena os resultados pela quantidade total de pedidos (totalQuantityOrdered), em order decrescente.
-- TOP 1 limita o resultado ao item mais pedido.


-- 2.	Crie uma consulta que retorne todos os clientes que realizaram mais de 4 pedidos no último ano em ordem decrescente.

WITH OrdersLastYear AS (
	SELECT
		o.customerId,
		COUNT(*) AS totalOrders
	FROM
		[master].[dbo].[tb_orders] o
	WHERE
		o.orderDate >= DATEADD(year, -1, GETDATE())
	GROUP BY
		o.customerId
)
SELECT 
	c.customerId,
    c.firstName,
    c.lastName,
    c.customerDoc,
    c.birthDate,
    oly.totalOrders
FROM 
	OrdersLastYear oly
JOIN
	[master].[dbo].[tb_customers] c ON oly.customerId = c.customerId
WHERE
	oly.totalOrders > 4
ORDER BY
	oly.totalOrders DESC;

-- Explicação da Consulta
-- WITH OrdersLastYear AS cria uma subconsulta que seleciona os pedidos realizados no último ano e conta o úmero de pedidos por cliente.
-- o.orderDate >= DATEADD(year, -1, GETDATE()) filtra os pedidos realizados no último ano.
-- COUNT(*) AS totalOrders conta o número de pedidos por cliente.
-- GROUP BY o.customerId agrupa por customerId


-- 3.	Crie uma consulta de quantos pedidos foram realizados em cada mês do último ano.

SELECT 
	DATEPART(year, o.orderDate) AS OrderYear,
	DATEPART(month, o.orderDate) AS OrderMonth,
	Count(*) AS TotalOrders
FROM 
	[master].[dbo].[tb_orders] o
WHERE 
	o.orderDate >= DATEADD(year, -1, GETDATE())
GROUP BY 
	DATEPART(year, o.orderDate),
    DATEPART(month, o.orderDate)
ORDER BY 
    OrderYear,
    OrderMonth;

-- Explicação da Consulta
-- Seleciona o ano e o mês dos pedidos com a função DATEPART.
-- OrderYear extrai o ano da data do pedido.
-- OrderMonth extrai o mês da data do pedido.
-- TotalOrders conta o número de pedidos para cada combinação de ano e mês.
-- FROM especifica a tabela de pedidos.
-- WHERE filtra os pedidos realizados no último ano.
-- GROUP BY agrupa os resultados por ano e mês.
-- ORDER BY ordena os resultados por ano e mês.


-- 4.	Crie uma consulta que retorne APENAS os campos "productName" e "totalAmount" dos 5 produtos mais pedidos.

SELECT TOP 5
    p.productName,
    SUM(oi.quantity) AS totalAmount
FROM
    [master].[dbo].[tb_order_items] oi
JOIN
    [master].[dbo].[tb_products] p ON oi.productId = p.productId
GROUP BY
    p.productName
ORDER BY
    totalAmount DESC;

-- Explicação da Consulta
-- JOIN uni a tabela tb_order_items com a tabela tb_products usando a coluna productId.
-- SUM calcula a quantidade total de cada produto pedido.
-- GROUP BY agrupa por productName.
-- ORDER BY ordena os resultados pela quantidade total de pedidos, em order decrscente.
-- TOP 5 limita aos 5 produtos mais pedidos.


-- 5.	Crie uma consulta liste todos os clientes que não realizaram nenhum pedido.

SELECT 
    c.customerId,
    c.customerDoc,
    c.firstName,
    c.lastName,
    c.birthDate
FROM 
    [master].[dbo].[tb_customers] c
LEFT JOIN 
    [master].[dbo].[tb_orders] o ON c.customerId = o.customerId
WHERE 
    o.orderId IS NULL;

-- Explicação da Consulta
-- LEFT JOIN junção a esquerda da tabela tb_customers e a tb_orders usando a coluna customerId, assim garantimos que todos os clientes sejam incluídos.
-- WHERE filtra os resultados incluindo apenas os clientes que não tem correspondência na tabela tb_orders, clientes que não realizaram nenhum pedido.


-- 6.	Crie uma consulta que retorne a data e o nome do produto do último pedido realizado pelos clientes onde o customerId são 94, 130, 300 e 1000.

WITH LastOrders AS (
    SELECT 
        o.customerId,
        o.orderId,
        o.orderDate,
        ROW_NUMBER() OVER (PARTITION BY o.customerId ORDER BY o.orderDate DESC) AS rn
    FROM 
        [master].[dbo].[tb_orders] o
    WHERE 
        o.customerId IN (94, 130, 300, 1000)
)
SELECT 
	customerId,
    lo.orderDate,
    p.productName
FROM 
    LastOrders lo
JOIN 
    [master].[dbo].[tb_order_items] oi ON lo.orderId = oi.orderId
JOIN 
    [master].[dbo].[tb_products] p ON oi.productId = p.productId
WHERE 
    lo.rn = 1;

-- Explicação da Consulta
-- WITH cria uma subconsulta para identificar o último pedido de cada cliente específico.
-- ROW_NUMBER usada para numerar os pedidos de cada cliente em order decrescente pela data do pedido. O pedido mais recente recebe rn = 1.
-- WHERE filtra os pedidos realizados pelos clientes específicos.
-- SELECT ... FROM seleciona a data do pedido e o nome do produto.
-- 1° JOIN junta a 	LastOrders com a tabela tb_order_items para obter os itens do pedido.
-- 2° JOIN junta a tabela tb_order_items com tb_products para obter o nome dos produtos.
-- WHERE filtra para incluir apenas o último pedido de cada cliente.


/* 7.	Com base na estrutura das tabelas fornecidas (tb_order_items, tb_orders, tb_products, tb_customers), crie uma nova tabela para armazenar informações sobre vendedores. 
A tabela deve seguir os conceitos básicos de modelo relacional. Certifique-se de definir claramente as colunas da tabela e suas relações com outras tabelas, se aplicável. */

CREATE TABLE [Concentrix].[dbo].[tb_sellers] (
    sellerId INT IDENTITY(1,1) PRIMARY KEY,
    firstName NVARCHAR(50) NOT NULL,
    lastName NVARCHAR(50) NOT NULL,
    hireDate DATE NOT NULL,
    email NVARCHAR(100) UNIQUE NOT NULL
);


-- 8.	Crie uma procedure que insira dados na tabela de vendedores criada anteriormente.

USE Concentrix;
GO

DROP PROCEDURE IF EXISTS [dbo].[InsertSeller];
GO

CREATE PROCEDURE [dbo].[InsertSeller]
    @firstName NVARCHAR(50),
    @lastName NVARCHAR(50),
    @hireDate DATE,
    @email NVARCHAR(100),
    @status INT OUTPUT,
    @message NVARCHAR(250) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- Verifica se o vendedor já existe com base no email
    IF EXISTS (SELECT 1 FROM [dbo].[tb_sellers] WHERE email = @email)
    BEGIN
        -- Vendedor ja existe
        SET @status = 1;
        SET @message = 'Vendedor já existe na tabela.';
        RETURN;
    END
    
    -- Insere um novo vendedor
    INSERT INTO [dbo].[tb_sellers] (firstName, lastName, hireDate, email)
    VALUES (@firstName, @lastName, @hireDate, @email);
    
    -- Checa se a inserção foi bem sucedida
    IF @@ROWCOUNT > 0
    BEGIN
        SET @status = 0;
        SET @message = 'Inserção bem-sucedida.';
    END
    ELSE
    BEGIN
        SET @status = 2;
        SET @message = 'Erro ao inserir o vendedor.';
    END
END;
GO

-- Explicação da Consulta
-- DROP PROCEDURE IF remove a procedure se ela existir
-- CREATE PROCEDURE define os parâmetros de entrada e saída para o novo vendedor
-- SET NOCOUNT ON melhora a performace, desativando a mensagem de contagem de linhas afetadas
-- PROCEDURE garante que não haverá vendedores duplicados com base no email
-- Melhora a eficiência ao desativar mensagens de contagem de linhas

--------------- QUESTÕES EM PYTHON	-------------------

/* 9.	Escreva um código em Python que se conecte a um banco de dados SQL Server e chame a procedure criada anteriormente para inserir um novo vendedor na tabela criada. 
Certifique-se de incluir o código de conexão ao banco de dados e a chamada da procedure com os parâmetros corretos. */

!pip install pyodbc

import pyodbc

# Informações de conexão
server = 'DESKTOP-5CJ61AF'  # Nome do servidor
database = 'Concentrix'  # Nome do banco de dados
driver = '{ODBC Driver 17 for SQL Server}'  # Driver ODBC

# String de conexão para autenticação do Windows
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    # Conectar ao banco de dados
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Preparar os parâmetros de entrada
    first_name = 'Carlos'
    last_name = 'Silva'
    hire_date = '2024-07-29'
    email = 'carlos.silva@example.com'

    # Preparar os parâmetros de saída
    status = 0
    message = ''

    # Preparar a chamada para a stored procedure com parâmetros de saída
    sql = "{CALL dbo.InsertSeller (?, ?, ?, ?, ?, ?)}"
    
    # Chamar a stored procedure
    cursor.execute(sql, (first_name, last_name, hire_date, email, status, message))

    # Recuperar os valores de saída
    cursor.execute("DECLARE @status INT, @message NVARCHAR(250); EXEC dbo.InsertSeller ?, ?, ?, ?, @status OUTPUT, @message OUTPUT; SELECT @status, @message;", 
                   (first_name, last_name, hire_date, email))

    result = cursor.fetchone()

    if result:
        output_status = result[0]
        output_message = result[1]
        print(f"Status: {output_status}, Message: {output_message}")
    else:
        print("Erro ao recuperar o status e a mensagem.")

    # Confirmar a transação
    connection.commit()

except pyodbc.Error as e:
    print("Erro ao conectar ao banco de dados:", e)

finally:
    # Fechar a conexão
    if connection:
        cursor.close()
        connection.close()

-- Explicação da Consulta
-- Declara variáveis de saída @status e @message.
-- Executa a stored procedure InsertSeller com os parâmetros de entrada e define @status e @message como variáveis de saída.
-- Seleciona os valores das variáveis de saída.
-- Usa fetchone para obter a linha de resultados contendo os valores de saída.
-- Verifica se resultado contém dados e imprime os valores de saída.


-- 10.	Em Python, crie um código que carregue em um “data frame” a tabela pedidos e a partir dele retorne os 10 produtos mais pedidos com as colunas "productName" e "numberOfOrders" em ordem decrescente.

import pandas as pd

# Informações de conexão
server = 'DESKTOP-5CJ61AF'  # Nome do servidor
database = 'Concentrix'  # Nome do banco de dados
driver = '{ODBC Driver 17 for SQL Server}'  # Driver ODBC

# String de conexão para autenticação do Windows
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    # Conectar ao banco de dados
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Consulta para carregar a tabela de pedidos com os produtos
    query = '''
    SELECT p.productName, COUNT(oi.orderItemId) AS numberOfOrders
    FROM tb_order_items oi
    JOIN tb_products p ON oi.productId = p.productId
    GROUP BY p.productName
    ORDER BY numberOfOrders DESC
    '''

    # Executar a consulta e carregar os dados em um DataFrame
    df = pd.read_sql(query, connection)

    # Selecionar os 10 produtos mais pedidos
    top_10_products = df.head(10)

    print(top_10_products)

except pyodbc.Error as e:
    print("Erro ao conectar ao banco de dados:", e)

finally:
    # Fechar a conexão
    if connection:
        cursor.close()
        connection.close()

-- Resultado da consulta

productName  numberOfOrders
0  External Hard Drive            9090
1      Fitness Tracker            9090
2           Headphones            9090
3             Keyboard            9090
4              Monitor            9090
5                Mouse            9090
6             Notebook            9090
7              Printer            9090
8               Router            9090
9           Smartphone            9090

-- Seleciona o nome do produto e a contagem de itens de pedido agrupados por nome de produto.
-- Resultados são ordenados em ordem decrescente pelo número de pedidos.
-- Carregamento do DataFrame:
-- Consulta executada e carregada os dados em um DataFrame usando pandas.
-- Seleção dos 10 Produtos Mais Pedidos:
-- Os 10 primeiros produtos do DataFrame, que são os mais pedidos.


