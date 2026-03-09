import streamlit as st
import pandas as pd
import os
import glob

st.set_page_config(page_title="Global Job Market Pipeline", layout="wide")

st.title("🌍 Global Job Market — Data Engineering Dashboard")
st.caption("Automated pipeline | Data refreshed daily via GitHub Actions")

# DEFINA OS CAMINHOS ANTES DE USAR
gold_path = "data/gold"
salary_file = os.path.join(gold_path, "salary_by_country.csv")
seniority_file = os.path.join(gold_path, "jobs_by_seniority.csv")

salary_df = pd.read_csv(salary_file) if os.path.exists(salary_file) else None
seniority_df = pd.read_csv(seniority_file) if os.path.exists(seniority_file) else None

# Em vez de confiar apenas no glob, vamos buscar o arquivo mais recente de forma direta
silver_dir = "data/silver"
files = [os.path.join(silver_dir, f) for f in os.listdir(silver_dir) if f.endswith('.csv')]

if not files:
    st.error("Nenhum arquivo CSV encontrado em data/silver/")
    st.stop()

# Pega o arquivo mais recente
latest_file = max(files, key=os.path.getctime)
df = pd.read_csv(latest_file)

salary_df = pd.read_csv(salary_file) if os.path.exists(salary_file) else None
seniority_df = pd.read_csv(seniority_file) if os.path.exists(seniority_file) else None

# Métricas no topo
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Jobs", len(df))
col2.metric("Countries", df['country'].nunique())
col3.metric("Companies", df['company_name'].nunique())
col4.metric("Avg Salary (USD)", f"${df[df['salary_min']>0]['salary_min'].mean():,.0f}")

st.divider()

# Gráficos
col1, col2 = st.columns(2)

with col1:
    st.subheader("Jobs by Country")
    st.bar_chart(df['country'].value_counts())

with col2:
    st.subheader("Seniority Distribution")
    st.bar_chart(df['seniority'].value_counts())

if salary_df is not None:
    st.subheader("Average Salary by Country & Seniority")
    st.dataframe(salary_df, use_container_width=True)

st.subheader("Latest Job Postings")
cols = ['title','company_name','country','seniority','salary_min','salary_max', 'redirect_url']

# Use o data_editor para permitir links clicáveis
st.data_editor(
    df[cols].sort_values('salary_min', ascending=False).head(50),
    column_config={
        "redirect_url": st.column_config.LinkColumn("Link da Vaga")
    },
    hide_index=True,
    use_container_width=True
)