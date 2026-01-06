import duckdb
import pandas as pd
#pd.set_option('display.width', 80) # Limita a largura do texto
#pd.set_option('display.max_columns', 5) # Mostra poucas colunas por vez
# %%
# 1. Conectar ao banco
con = duckdb.connect('database_0.db')
con.execute("DROP TABLE IF EXISTS base")
# %%
# 2. Criar a tabela (Comando em linha única para evitar erro de indentação)
con.execute("CREATE TABLE base AS SELECT * FROM read_csv_auto('/home/emd/Documents/data-analysis/data/*.csv', union_by_name=True, encoding='ISO_8859_1')")
# %%
# 3. Visualizar
df = con.execute("SELECT * FROM base").df()
print(df)
# %%
# 4. Conferindo colunas
for col in df.columns:
    if df[col].dtype == 'object':
        # Tenta ver se o primeiro valor não nulo parece um número
        exemplo = str(df[col].dropna().iloc[0])
        if exemplo.replace('.','',1).replace(',','',1).isdigit():
            print(f"A coluna [{col}] parece numérica mas está como OBJECT")
# %%
# Criamos a query
query = '''
    (SELECT 'Cod. Favorecido' as Col, "Código favorecido" as Val FROM df LIMIT 20)
    UNION ALL
    (SELECT 'Cod. Autor' as Col, "Código do Autor da Emenda" as Val FROM df LIMIT 20)
    UNION ALL
    (SELECT 'Num. Emenda' as Col, "Número da emenda" as Val FROM df LIMIT 20)
    UNION ALL
    (SELECT 'Vl. Empenhado' as Col, "Valor Empenhado" as Val FROM df LIMIT 20)
    UNION ALL
    (SELECT 'Vl. Cancelado' as Col, "Valor Cancelado" as Val FROM df LIMIT 20)
    UNION ALL
    (SELECT 'Vl. Pago' as Col, "Valor Pago" as Val FROM df LIMIT 20)
'''

# Convertemos para DataFrame e forçamos o print de TODAS as linhas geradas
print(con.sql(query).df().to_string(index=False))
# %%
# Executa a transformação pesada direto no banco
con.execute("""
    CREATE OR REPLACE TABLE base AS
    SELECT
        * EXCLUDE ("Valor Pago", "Valor Empenhado", "Valor Cancelado", "Código favorecido", "Código do Autor da Emenda", "Número da emenda"),

        -- Converte financeiro: troca vírgula por ponto. Inválidos viram NULL.
        TRY_CAST(REPLACE("Valor Pago", ',', '.') AS DOUBLE) AS "Valor Pago",
        TRY_CAST(REPLACE("Valor Empenhado", ',', '.') AS DOUBLE) AS "Valor Empenhado",
        TRY_CAST(REPLACE("Valor Cancelado", ',', '.') AS DOUBLE) AS "Valor Cancelado",

        -- Código Favorecido: Negativos ou erros viram NULL
        CASE
            WHEN TRY_CAST("Código favorecido" AS BIGINT) < 0 THEN NULL
            ELSE TRY_CAST("Código favorecido" AS BIGINT)
        END AS "Código favorecido",

        -- Códigos e Números: Conversão direta para Inteiro (BIGINT para segurança)
        TRY_CAST("Código do Autor da Emenda" AS BIGINT) AS "Código do Autor da Emenda",
        TRY_CAST("Número da emenda" AS BIGINT) AS "Número da emenda"
    FROM base
""")

# Limpa o cache e otimiza o arquivo .db no disco
con.execute("VACUUM")

# Atualiza o seu DataFrame na memória para refletir as mudanças
df = con.table('base').df()

print("Banco 'base' higienizado!")
print(df.info())
# %%
print(df.info())
# %%
# Lista todas as colunas da tabela 'base'
todas_colunas = con.sql("DESCRIBE base").df()['column_name'].tolist()

print(f"Iniciando inspeção de {len(todas_colunas)} colunas...")

for col in todas_colunas:
    print(f"\n{'='*40}")
    print(f"COLUNA: {col}")
    print(f"{'='*40}")

    # Pegamos 10 exemplos, ignorando nulos para ver conteúdo real
    query = f'SELECT "{col}" FROM base WHERE "{col}" IS NOT NULL LIMIT 10'

    try:
        amostra = con.sql(query).df()
        if amostra.empty:
            print("[Coluna totalmente vazia ou apenas com NULLs]")
        else:
            print(amostra.to_string(index=False, header=False))
    except Exception as e:
        print(f"Erro ao ler coluna {col}: {e}")
# %%
con.execute("""
    CREATE OR REPLACE TABLE base AS
    SELECT
        -- 1. Limpeza de Strings (Removendo espaços extras)
        CAST("Código Apoiador" AS BIGINT) AS "Código Apoiador",
        TRIM(Apoiador) AS Apoiador,
        "Data do Apoio",
        "Data Retirada do Apoio",
        TRIM(Empenho) AS Empenho,
        TRIM(Favorecido) AS Favorecido,
        TRIM("Tipo Favorecido") AS "Tipo Favorecido",
        TRIM("UF Favorecido") AS "UF Favorecido",
        TRIM("Município Favorecido") AS "Município Favorecido",
        "Código da Emenda",
        TRIM("Tipo de Emenda") AS "Tipo de Emenda",
        "Ano da Emenda",
        TRIM("Nome do Autor da Emenda") AS "Nome do Autor da Emenda",
        TRIM("Localidade de aplicação do recurso") AS "Localidade de aplicação do recurso",
        TRIM(UG) AS UG,
        TRIM("Unidade Orçamentária") AS "Unidade Orçamentária",
        TRIM(Órgão) AS Órgão,
        TRIM("Órgão Superior") AS "Órgão Superior",
        TRIM(Ação) AS Ação,
        TRIM("Código Ação") AS "Código Ação",

        -- 2. Tratando o -1 como NULL (Transformando em Inteiros)
        NULLIF("Código UG", -1) AS "Código UG",
        NULLIF("Código Unidade Orçamentária", -1) AS "Código Unidade Orçamentária",
        NULLIF("Código Órgão SIAFI", -1) AS "Código Órgão SIAFI",
        NULLIF("Código Órgão Superior SIAFI", -1) AS "Código Órgão Superior SIAFI",

        -- Colunas que o Pandas já leu como Int64/float64
        "Código favorecido",
        "Código do Autor da Emenda",
        "Número da emenda",
        "Valor Pago",
        "Valor Empenhado",
        "Valor Cancelado",

        -- 3. Convertendo Data Brasileira (Texto -> Date)
        TRY_CAST(
            strptime(
                NULLIF(TRIM("Data última movimentação Empenho"), 'Sem informação'),
                '%d/%m/%Y'
            ) AS DATE
        ) AS "Data última movimentação Empenho"

    FROM base
""")

con.execute("VACUUM")
df = con.table('base').df()
print("✅ Tudo pronto! Agora as datas e códigos estão limpos.")
# %%
# Resumo estatístico das colunas financeiras
resumo = df[['Valor Empenhado', 'Valor Pago', 'Valor Cancelado']].describe().round(2)
print("--- Estatísticas Financeiras Gerais ---")
print(resumo)

# Cálculo da taxa de execução (Quanto do empenhado foi efetivamente pago)
total_pago = df['Valor Pago'].sum()
total_empenhado = df['Valor Empenhado'].sum()
taxa_execucao = (total_pago / total_empenhado) * 100

print(f"\nTotal Empenhado: R$ {total_empenhado:,.2f}")
print(f"Total Pago: R$ {total_pago:,.2f}")
print(f"Taxa de Execução: {taxa_execucao:.2f}%")
# %%
resumo_uf = df.groupby('UF Favorecido')['Valor Pago'].agg(['sum', 'count']).sort_values(by='sum', ascending=False)
print(resumo_uf.head(10).apply(lambda x: f"R$ {x:,.2f}" if isinstance(x, float) else x))
# %%
top_orgaos = df.groupby('Órgão Superior')['Valor Pago'].sum().sort_values(ascending=False)
print(top_orgaos.head(10).apply(lambda x: f"R$ {x:,.2f}"))
# %%
import matplotlib.pyplot as plt

# Preparando os dados
resumo_uf = df.groupby('UF Favorecido')['Valor Pago'].sum().sort_values(ascending=False).head(15)

# Criando o gráfico
plt.figure(figsize=(12, 6))
resumo_uf.plot(kind='bar', color='skyblue')

plt.title('Top 15 Estados por Valor Pago (R$)', fontsize=14)
plt.xlabel('Estado (UF)', fontsize=12)
plt.ylabel('Bilhões de Reais', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Ajusta o layout e salva
plt.tight_layout()
plt.savefig('pagamentos_uf.png')
print("📊 Gráfico salvo como 'pagamentos_uf.png'!")
# %%
# Filtrando apenas o que está "Sem informação" no Órgão Superior
df_fantasma = df[df['Órgão Superior'] == 'Sem informação']

# Verificando os maiores favorecidos desse R$ 1.5 bi
print("--- Maiores Favorecidos (Órgão Superior Sem Informação) ---")
resumo_fantasma = df_fantasma.groupby(['Favorecido', 'UF Favorecido'])['Valor Pago'].sum().sort_values(ascending=False).head(10)
print(resumo_fantasma.apply(lambda x: f"R$ {x:,.2f}"))
# %%
# Verificando os valores únicos que contêm "Sem" ou são curtos
print("--- Valores exatos encontrados na coluna Órgão Superior ---")
print(df['Órgão Superior'].value_counts().filter(like='Sem').head())
# %%
# O \s+ captura qualquer tipo de espaço (espaço comum, tab, non-breaking space, etc)
df_fantasma = df[df['Órgão Superior'].str.contains(r'Sem\s+informação', regex=True, na=False)]

print(f"Total de linhas capturadas: {len(df_fantasma)}")
print(f"Soma total validada: R$ {df_fantasma['Valor Pago'].sum():,.2f}")

if not df_fantasma.empty:
    print("\n--- Destino do R$ 1.5 Bi (Top 10 Favorecidos) ---")
    # Agrupando por Favorecido e UF para ver quem são os grandes recebedores
    resumo = df_fantasma.groupby(['Favorecido', 'UF Favorecido'])['Valor Pago'].sum().sort_values(ascending=False).head(10)
    print(resumo.apply(lambda x: f"R$ {x:,.2f}"))

    print("\n--- Qual o 'Tipo de Emenda' desses casos? ---")
    print(df_fantasma['Tipo de Emenda'].value_counts())
# %%
# Vamos olhar as tripas dessas 1372 linhas
print("--- Amostra bruta das linhas 'Sem Informação' ---")
print(df_fantasma[['Favorecido', 'UF Favorecido', 'Valor Pago', 'Ação']].head(15))

# Verificando se o Favorecido também é "Sem informação"
print("\n--- Conteúdo da coluna Favorecido nestes casos ---")
print(df_fantasma['Favorecido'].value_counts().head())
## %
# Pegamos a lista de códigos de empenho das linhas "fantasmas"
empenhos_fantasmas = df_fantasma['Empenho'].unique()

# Buscamos na base completa se esses mesmos empenhos aparecem em linhas que TÊM favorecido
rastreio = df[
    (df['Empenho'].isin(empenhos_fantasmas)) &
    (df['Favorecido'] != 'Sem informação')
]

if not rastreio.empty:
    print(f"🎯 Sucesso! Encontramos o rastro de {rastreio['Empenho'].nunique()} empenhos.")
    print("\n--- Identidade Real dos Favorecidos 'Ocultos' ---")
    resumo_real = rastreio.groupby('Favorecido')['Valor Pago'].sum().sort_values(ascending=False).head(10)
    print(resumo_real.apply(lambda x: f"R$ {x:,.2f}"))
else:
    print("❌ Os empenhos fantasmas realmente só existem em linhas sem informação.")
# %%
# Verificando quem são os apoiadores dessas 1.372 linhas fantasmas
print("--- Quem são os Apoiadores deste R$ 1.5 Bi Oculto? ---")
apoiadores_fantasma = df_fantasma.groupby('Apoiador')['Valor Pago'].agg(['sum', 'count']).sort_values(by='sum', ascending=False)

# Formatando para facilitar a leitura
apoiadores_fantasma['sum'] = apoiadores_fantasma['sum'].apply(lambda x: f"R$ {x:,.2f}")
print(apoiadores_fantasma.head(15))
# %%
# 1. Total enviado por político (Total da base)
total_por_politico = df.groupby('Apoiador')['Valor Pago'].sum()

# 2. Total "Oculto" por político (usando o nosso df_fantasma)
oculto_por_politico = df_fantasma.groupby('Apoiador')['Valor Pago'].sum()

# 3. Criando o Ranking de Opacidade
ranking_opacidade = pd.DataFrame({
    'Total Enviado': total_por_politico,
    'Total Oculto': oculto_por_politico
}).fillna(0)

ranking_opacidade['% Opacidade'] = (ranking_opacidade['Total Oculto'] / ranking_opacidade['Total Enviado'] * 100).round(2)

# Exibir os 10 políticos com mais dinheiro oculto em termos percentuais (mínimo de R$ 10 milhões enviados)
print("--- Ranking de Opacidade (Políticos com > R$ 10mi enviados) ---")
resultado = ranking_opacidade[ranking_opacidade['Total Enviado'] > 10_000_000].sort_values(by='Total Oculto', ascending=False).head(15)

# Formatação para R$
for col in ['Total Enviado', 'Total Oculto']:
    resultado[col] = resultado[col].apply(lambda x: f"R$ {x:,.2f}")

print(resultado)
# %%
import duckdb

# Conecta ao banco físico que será seu "produto final"
con = duckdb.connect('emendas_analytics.db')

# Criando a Silver com TODAS as correções que discutimos
con.execute("""
    CREATE OR REPLACE TABLE silver_emendas AS
    SELECT
        -- 1. Strings limpas (sem espaços invisíveis)
        TRIM(Apoiador) AS Apoiador,
        TRIM(Empenho) AS Empenho,
        TRIM(Favorecido) AS Favorecido,
        TRIM("UF Favorecido") AS UF_Favorecido,
        TRIM("Município Favorecido") AS Municipio_Favorecido,
        TRIM("Tipo de Emenda") AS Tipo_Emenda,
        TRIM("Nome do Autor da Emenda") AS Autor_Emenda,
        TRIM("Órgão Superior") AS Orgao_Superior,
        TRIM(Ação) AS Acao,

        -- 2. Datas convertidas (de texto para DATE real)
        "Data do Apoio",
        "Data Retirada do Apoio",
        TRY_CAST(strptime(NULLIF(TRIM("Data última movimentação Empenho"), 'Sem informação'), '%d/%m/%Y') AS DATE) AS Data_Ultima_Movimentacao,

        -- 3. Códigos e Valores (Tratando nulos e mantendo números)
        NULLIF("Código UG", -1) AS Codigo_UG,
        NULLIF("Código Unidade Orçamentária", -1) AS Codigo_UO,
        "Valor Pago",
        "Valor Empenhado",
        "Valor Cancelado",
        "Ano da Emenda",
        "Código favorecido",

        -- 4. Flag de Opacidade (Sua regra de negócio)
        CASE WHEN "Órgão Superior" LIKE '%Sem informação%' THEN TRUE ELSE FALSE END AS eh_opaco

    FROM bronze_emendas;
""")

# Agora criamos a Gold baseada na Silver já limpa
con.execute("""
    CREATE OR REPLACE TABLE gold_resumo_financeiro AS
    SELECT
        Apoiador,
        UF_Favorecido,
        SUM("Valor Pago") AS total_pago,
        COUNT(*) AS qtd_emendas
    FROM silver_emendas
    GROUP BY ALL
    ORDER BY total_pago DESC;
""")

print("✨ Sucesso! Colunas limpas na Silver e resumo criado na Gold.")
con.close()
# %%
import duckdb

# 1. Conecta ao banco físico
con = duckdb.connect('emendas_analytics.db')

# 2. O PULO DO GATO: Registra o seu DataFrame atual 'df' como uma tabela temporária
# para que o SQL consiga enxergá-lo
con.register('df_python', df)

# 3. Cria a camada Bronze real dentro do arquivo .db
con.execute("CREATE OR REPLACE TABLE bronze_emendas AS SELECT * FROM df_python")

# 4. Agora sim, cria a Silver (o seu código anterior com o FROM corrigido)
con.execute("""
    CREATE OR REPLACE TABLE silver_emendas AS
    SELECT
        TRIM(Apoiador) AS Apoiador,
        TRIM(Empenho) AS Empenho,
        TRIM(Favorecido) AS Favorecido,
        TRIM("UF Favorecido") AS UF_Favorecido,
        TRIM("Município Favorecido") AS Municipio_Favorecido,
        TRIM("Tipo de Emenda") AS Tipo_Emenda,
        TRIM("Nome do Autor da Emenda") AS Autor_Emenda,
        TRIM("Órgão Superior") AS Orgao_Superior,
        TRIM(Ação) AS Acao,
        "Data do Apoio",
        "Data Retirada do Apoio",
        TRY_CAST(strptime(NULLIF(TRIM("Data última movimentação Empenho"), 'Sem informação'), '%d/%m/%Y') AS DATE) AS Data_Ultima_Movimentacao,
        NULLIF("Código UG", -1) AS Codigo_UG,
        NULLIF("Código Unidade Orçamentária", -1) AS Codigo_UO,
        "Valor Pago",
        "Valor Empenhado",
        "Valor Cancelado",
        "Ano da Emenda",
        "Código favorecido",
        CASE WHEN "Órgão Superior" LIKE '%Sem informação%' THEN TRUE ELSE FALSE END AS eh_opaco
    FROM bronze_emendas;
""")

# 5. Cria a Gold
con.execute("""
    CREATE OR REPLACE TABLE gold_resumo_financeiro AS
    SELECT
        Apoiador,
        UF_Favorecido,
        SUM("Valor Pago") AS total_pago,
        COUNT(*) AS qtd_emendas
    FROM silver_emendas
    GROUP BY ALL
    ORDER BY total_pago DESC;
""")

print("✨ Agora sim! Banco 'emendas_analytics.db' persistido com Bronze, Silver e Gold.")
con.close()
# %%
import duckdb

con = duckdb.connect('emendas_analytics.db')
con.register('df_python', df)

# 1. Recriando a Bronze
con.execute("CREATE OR REPLACE TABLE bronze_emendas AS SELECT * FROM df_python")

# 2. Criando a Silver com ajustes de tipo
con.execute("""
    CREATE OR REPLACE TABLE silver_emendas AS
    SELECT
        -- Colunas que garantidamente são strings
        TRIM(CAST(Apoiador AS VARCHAR)) AS Apoiador,
        TRIM(CAST(Empenho AS VARCHAR)) AS Empenho,
        TRIM(CAST(Favorecido AS VARCHAR)) AS Favorecido,
        TRIM(CAST("UF Favorecido" AS VARCHAR)) AS UF_Favorecido,
        TRIM(CAST("Município Favorecido" AS VARCHAR)) AS Municipio_Favorecido,
        TRIM(CAST("Tipo de Emenda" AS VARCHAR)) AS Tipo_Emenda,
        TRIM(CAST("Nome do Autor da Emenda" AS VARCHAR)) AS Autor_Emenda,
        TRIM(CAST("Órgão Superior" AS VARCHAR)) AS Orgao_Superior,
        TRIM(CAST(Ação AS VARCHAR)) AS Acao,

        -- Datas: Se já forem TIMESTAMP, apenas mantemos.
        -- Se houver erro, o DuckDB nos dirá, mas aqui vamos apenas passar a coluna
        "Data do Apoio",
        "Data Retirada do Apoio",
        "Data última movimentação Empenho" AS Data_Ultima_Movimentacao,

        -- Números e Códigos
        NULLIF("Código UG", -1) AS Codigo_UG,
        NULLIF("Código Unidade Orçamentária", -1) AS Codigo_UO,
        "Valor Pago",
        "Valor Empenhado",
        "Valor Cancelado",
        "Ano da Emenda",
        "Código favorecido",

        CASE WHEN CAST("Órgão Superior" AS VARCHAR) LIKE '%Sem informação%' THEN TRUE ELSE FALSE END AS eh_opaco
    FROM bronze_emendas;
""")

# 3. Criando a Gold (Resumo)
con.execute("""
    CREATE OR REPLACE TABLE gold_resumo_financeiro AS
    SELECT
        Apoiador,
        UF_Favorecido,
        SUM("Valor Pago") AS total_pago,
        COUNT(*) AS qtd_emendas
    FROM silver_emendas
    GROUP BY ALL
    ORDER BY total_pago DESC;
""")

print("✨ Banco consolidado com sucesso!")
con.close()
# %%
import duckdb

con = duckdb.connect('emendas_analytics.db')

# Lista todas as tabelas no banco
print("--- Tabelas (Camadas) no DB ---")
print(con.execute("SHOW TABLES").df())

con.close()
