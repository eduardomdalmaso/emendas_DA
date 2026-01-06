# 📊 Análise de Emendas Parlamentares (2020-2026)

Este projeto é uma ferramenta de **Data Engineering** e **Analytics** desenvolvida para monitorar a transparência na alocação de recursos públicos brasileiros. O foco principal é identificar o nível de rastreabilidade (opacidade) das emendas parlamentares.

## 🛠️ Tecnologias Utilizadas
- **Linguagem:** Python 3.10+
- **Banco de Dados:** DuckDB (Processamento OLAP de alta performance)
- **Dashboard:** Streamlit & Plotly
- **Automação:** Requests & Selenium (Extração direta do Portal da Transparência)

## 📁 Arquitetura de Dados
O projeto utiliza a arquitetura de medalhão:
1. **Bronze:** Dados brutos baixados via script.
2. **Silver:** Dados higienizados, com tipos convertidos e tratamento de strings/nulos via SQL no DuckDB.
3. **Gold:** Agregações financeiras otimizadas para o dashboard.

## 🚀 Como Executar
1. Clone o repositório: `git clone https://github.com/eduardomdalmaso/emendas_DA`
2. Instale as dependências: `pip install -r requirements.txt`
3. Baixe os dados: `python scripts/extrair.py`
4. Gere o banco: `python scripts/build_database.py`
5. Rode o Dashboard: `streamlit run Investigacao.py`

## ⚖️ Isenção de Responsabilidade
Esta é uma análise técnica baseada exclusivamente em dados públicos. O termo "Opacidade" refere-se à ausência de preenchimento de metadados (como município destino) nos registros oficiais. Este trabalho não constitui acusação criminal, mas sim um exercício de transparência e controle social.

**Licença:** MIT
