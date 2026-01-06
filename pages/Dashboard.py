import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# 1. Configuração da página (Sintaxe 2026 para Multipage)
st.set_page_config(page_title="Dashboard de Opacidade", layout="wide")

st.title("📊 Análise de Dados: Ranking e Estados")

# 2. Conexão com o banco de dados (Caminho para a raiz)
con = duckdb.connect('emendas_analytics.db')

# --- SIDEBAR (Filtros) ---
st.sidebar.header("Filtros")
anos_disponiveis = con.execute('SELECT DISTINCT "Ano da Emenda" FROM silver_emendas ORDER BY 1 DESC').df()
filtro_ano = st.sidebar.multiselect(
    "Selecione o(s) Ano(s):",
    options=anos_disponiveis["Ano da Emenda"].tolist(),
    default=anos_disponiveis["Ano da Emenda"].tolist()
)

if not filtro_ano:
    st.error("Por favor, selecione pelo menos um ano.")
    st.stop()

# --- CARREGAMENTO E PROCESSAMENTO ---
anos_str = ", ".join([str(ano) for ano in filtro_ano])
query_base = f'SELECT * FROM silver_emendas WHERE "Ano da Emenda" IN ({anos_str})'
df_filtrado = con.execute(query_base).df()

# --- KPI'S ---
total_geral = df_filtrado["Valor Pago"].sum()
total_oculto = df_filtrado[df_filtrado["eh_opaco"]]["Valor Pago"].sum()

def formatar_br_kpi(valor):
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

c1, c2, c3 = st.columns(3)
c1.metric("Total Analisado", formatar_br_kpi(total_geral))
c2.metric("Total 'Oculto' (Sem Rastro)", formatar_br_kpi(total_oculto), delta_color="inverse")
c3.metric("% de Opacidade Geral", f"{(total_oculto/total_geral)*100:.2f}%")

# --- GRÁFICOS ---
col_graf1, col_graf2 = st.columns(2)

with col_graf1:
    # Top 15 Políticos com mais verba opaca
    df_pol = df_filtrado.groupby("Apoiador")["Valor Pago"].agg(
        total_enviado="sum",
        total_oculto=lambda x: x[df_filtrado.loc[x.index, "eh_opaco"]].sum()
    ).reset_index().sort_values("total_oculto", ascending=True).tail(15)

    fig_pol = px.bar(df_pol, x='total_oculto', y='Apoiador', orientation='h',
                     title="Top 15 Políticos (Maior Volume Oculto)",
                     labels={'total_oculto': 'Valor Oculto (R$)', 'Apoiador': 'Parlamentar'},
                     color_discrete_sequence=['#d32f2f'])
    fig_pol.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_pol, width='stretch')

with col_graf2:
    # Top 10 Estados que mais receberam
    df_uf = df_filtrado.groupby("UF_Favorecido")["Valor Pago"].sum().reset_index().sort_values("Valor Pago", ascending=False).head(10)
    fig_uf = px.bar(df_uf, x='UF_Favorecido', y='Valor Pago',
                    title="Top 10 Estados (Recebimento Total)",
                    labels={'Valor Pago': 'Total Recebido (R$)', 'UF_Favorecido': 'Estado'},
                    color_discrete_sequence=['#1976d2'])
    st.plotly_chart(fig_uf, width='stretch')

# --- TABELA DETALHADA ---
st.divider()
st.subheader("🕵️ Detalhamento por Parlamentar")

busca = st.text_input("Pesquisar nome do parlamentar:", key="busca_dashboard")

df_tabela = df_filtrado.groupby("Apoiador").agg(
    total_enviado=("Valor Pago", "sum"),
    total_oculto=("Valor Pago", lambda x: x[df_filtrado.loc[x.index, "eh_opaco"]].sum())
).reset_index()

df_tabela["pct_opacidade"] = (df_tabela["total_oculto"] / df_tabela["total_enviado"] * 100).fillna(0)
df_tabela = df_tabela.sort_values("total_oculto", ascending=False)

if busca:
    df_tabela = df_tabela[df_tabela['Apoiador'].str.contains(busca, case=False, na=False)]

def formatar_real_tabela(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

df_display = df_tabela.copy()
df_display["total_enviado"] = df_display["total_enviado"].apply(formatar_real_tabela)
df_display["total_oculto"] = df_display["total_oculto"].apply(formatar_real_tabela)
df_display["pct_opacidade"] = df_display["pct_opacidade"].apply(lambda x: f"{x:.2f}%")

st.dataframe(
    df_display,
    column_config={
        "Apoiador": "Parlamentar",
        "total_enviado": "Total Pago",
        "total_oculto": "Valor Sem Rastro",
        "pct_opacidade": "% Opacidade"
    },
    width='stretch',
    hide_index=True
)

con.close()
