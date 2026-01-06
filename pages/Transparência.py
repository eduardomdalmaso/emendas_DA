import streamlit as st
import duckdb

st.set_page_config(page_title="Metodologia e Dicionário", layout="wide")

st.title("📖 Dicionário de Dados e Transparência")

st.markdown("""
Esta página detalha a origem dos dados, as definições técnicas e os critérios utilizados para a consolidação da base de dados unificada (2020-2024).
""")

st.divider()

# --- SEÇÃO 1: ORIGEM ---
st.header("🌐 Fonte dos Dados")
st.write("""
Os dados brutos foram extraídos do **Portal da Transparência do Governo Federal**, especificamente das bases de
**Emendas Parlamentares por Favorecido**.
* **Frequência de atualização original:** Diária.
* **Nossa base:** Consolidada com dados extraídos até o fechamento de 2024.
* **Link Oficial:** [Portal da Transparência](https://portaldatransparencia.gov.br/download-de-dados/apoiamento-emendas-parlamentares-documentos)
""")

# --- SEÇÃO 2: DICIONÁRIO ---
st.header("📊 Dicionário de Colunas")
st.markdown("""
Abaixo, explicamos o que cada termo representa nos nossos gráficos e tabelas:
""")

# Criando uma tabela de dicionário para ficar bem organizado
dados_dicionario = {
    "Coluna": ["Ano da Emenda", "Apoiador", "Valor Pago", "eh_opaco", "UF_Favorecido"],
    "Descrição": [
        "O ano orçamentário em que a emenda foi indicada.",
        "O parlamentar (Senador ou Deputado) que realizou a indicação do recurso.",
        "O valor financeiro que efetivamente saiu dos cofres públicos para o destino.",
        "Indicador lógico (Sim/Não) que identifica se a emenda possui baixo rastro de destino.",
        "Estado (Unidade da Federação) que recebeu o recurso."
    ],
    "Critério Técnico": [
        "Campo original do CSV.",
        "Nome normalizado para evitar duplicidades por erros de grafia.",
        "Consideramos apenas a fase de 'PAGAMENTO' para evitar valores apenas prometidos.",
        "Definido como 'Verdadeiro' quando o município ou beneficiário está vazio.",
        "Extraído do campo 'UF Beneficiário' da base original."
    ]
}

st.table(dados_dicionario)

st.divider()

# --- SEÇÃO 3: INTEGRIDADE ---
st.header("🛠️ Integridade e Processamento")
with st.expander("Clique para ver detalhes do processamento técnico"):
    st.write("""
    Para garantir a transparência do nosso processo de unificação:
    1. **Motor de Dados:** Utilizamos o **DuckDB** para unir 5 ficheiros CSV anuais em um único banco `.db`.
    2. **Limpeza:** Foram removidas linhas de estorno (valores negativos) que poderiam inflar os totais.
    3. **Rastreabilidade:** O script de processamento (`build_database.py`) garante que os dados exibidos aqui são cópias fiéis dos registos oficiais, sem qualquer alteração manual de valores.
    """)

st.info("A transparência pública é um pilar da democracia. Este projeto visa facilitar o acesso e a compreensão desses dados.")
