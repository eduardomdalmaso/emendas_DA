import streamlit as st

st.set_page_config(
    page_title="Análise de Emendas Parlamentares (2020-2024)",
    page_icon="📊",
    layout="wide"
)

# Título Principal
st.title("📊 Análise de Transparência: Emendas Parlamentares (2020-2024)")

st.subheader("🛡️ Rigor Técnico e Higienização")
st.write("""
Para garantir que os valores de **R$ 1,5 bilhão** fossem precisos, o pipeline de dados realizou:
* **Conversão de Tipos:** Transformação de valores monetários (padrão PT-BR) para numéricos.
* **Tratamento de Nulos:** Identificação de códigos '-1' ou 'Sem informação' como dados ausentes.
* **Arquitetura em Camadas:** Organização dos dados em **Bronze** (brutos), **Silver** (limpos) e **Gold** (agregados para o dashboard).
""")

# Introdução
st.markdown("""
Este projeto apresenta um levantamento técnico sobre o fluxo financeiro de emendas parlamentares no Brasil,
abrangendo o período de **2020 a 2024**. A análise foca na rastreabilidade dos recursos,
identificando o caminho desde a indicação parlamentar até o destino final nos municípios e estados.
""")

st.divider()

# Colunas para os principais achados do EDA
col1, col2 = st.columns(2)

with col1:
    st.subheader("🔍 Escopo da Análise")
    st.write("""
    * **Período:** 2020 a 2024.
    * **Volume Financeiro:** Monitoramento de repasses que somam bilhões de reais.
    * **Objetivo:** Quantificar o grau de rastreabilidade das verbas (identificação de beneficiários finais).
    """)

with col2:
    st.subheader("💡 O que os dados revelam")
    st.write("""
    * **Existência de Lacunas:** Uma parcela significativa dos recursos é paga sem que o destino municipal específico conste na base de dados principal.
    * **Consolidação:** O montante identificado com baixa rastreabilidade (sem destino declarado) atinge aproximadamente **R$ 1,5 bilhão**.
    """)

st.divider()

# Bloco de Metodologia Técnica (reforça que é um trabalho sério)
st.subheader("⚙️ Metodologia e Processamento")
st.markdown("""
A análise foi construída seguindo rigorosos padrões de ciência de dados para garantir a integridade dos resultados:
1. **Extração:** Dados brutos obtidos de fontes governamentais oficiais.
2. **Tratamento (Camada Silver):** Limpeza de duplicatas, padronização de nomes de parlamentares e tratamento de valores nulos utilizando **DuckDB**.
3. **Classificação de Opacidade:** Definimos como verbas 'Sem Rastro' aquelas em que o campo de município ou entidade beneficiária não foi preenchido ou consta como 'Não Informado'.
""")

# Chamada para ação
st.info("⬅️ **Utilize o menu lateral para navegar entre a metodologia detalhada e os dashboards interativos.**")

# Seção de Dicionário Rápido (reforça o tom técnico)
with st.expander("Consulte os termos técnicos utilizados"):
    st.write("""
    * **Empenhado:** Valor que o governo reservou para pagar.
    * **Pago:** Valor que efetivamente saiu da conta da União e foi para o favorecido.
    * **Apoiador:** Parlamentar responsável pela indicação da emenda.
    * **Opacidade/Sem Rastro:** Termo técnico para identificar a interrupção da rastreabilidade do destino final do recurso.
    """)
