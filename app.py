import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# Configuração da página
st.set_page_config(page_title="Global Job Market Dashboard", layout="wide")

# Conexão com o Banco (Usa a mesma lógica que criamos para o load.py)
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/jobs_db')
engine = create_engine(DATABASE_URL)

st.title("🌍 Global Job Market - Data Engineering")
st.markdown("Análise diária de vagas extraídas via API Adzuna")

try:
    # Carrega os dados da camada Silver/Gold
    df = pd.read_sql("SELECT * FROM jobs", engine)

    # Sidebar para Filtros
    st.sidebar.header("Filtros")
    selected_country = st.sidebar.multiselect("País", df['country'].unique(), default=df['country'].unique())
    
    # Filtrando os dados
    df_filtered = df[df['country'].isin(selected_country)]

    # Layout em Colunas
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Vagas por País")
        st.bar_chart(df_filtered['country'].value_counts())

    with col2:
        st.subheader("Média Salarial (Local)")
        # Removemos zeros para o cálculo da média não ser afetado
        salary_df = df_filtered[df_filtered['salary_min'] > 0]
        avg_salary = salary_df.groupby('country')['salary_min'].mean()
        st.bar_chart(avg_salary)

    st.subheader("Últimas Vagas Encontradas")
    
    # O bloco abaixo deve estar alinhado com o código acima (indentado)
    st.data_editor(
        df_filtered[['title', 'company_name', 'location_name', 'salary_min', 'redirect_url']].sort_values(by='salary_min', ascending=False),
        column_config={
            "redirect_url": st.column_config.LinkColumn("Link da Vaga")
        },
        hide_index=True
    )

except Exception as e:
    st.error(f"Erro ao conectar ao banco ou carregar dados: {e}")
    st.info("Certifique-se de que o Docker com o Postgres está rodando!")