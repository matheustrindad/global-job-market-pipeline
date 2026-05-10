import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Global Job Market Pipeline", layout="wide")
st.title("🌍 Global Job Market — Data Engineering Dashboard")
st.caption("Automated pipeline | Data refreshed daily via GitHub Actions")

# ── 1. CARREGAMENTO DOS DADOS ──────────────────────────────────────────────────
silver_dir = "data/silver"
files = [os.path.join(silver_dir, f) for f in os.listdir(silver_dir) if f.endswith('.csv')]

if not files:
    st.error("No CSV files found in data/silver/")
    st.stop()

# Pega o arquivo mais recente pelo nome (formato jobs_clean_YYYY-MM-DD.csv)
latest_file = sorted(files)[-1]
df = pd.read_csv(latest_file)

# ── 2. PARSE E FILTRO DE 28 DIAS ───────────────────────────────────────────────
if 'date_posted' in df.columns:
    # Parse simples sem timezone — o transform.py já salva como 'YYYY-MM-DD'
    df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')
    
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=28)
    df_active = df[df['date_posted'] >= cutoff].copy()
    
    if df_active.empty:
        st.warning("⚠️ No jobs found in the last 28 days. Showing full dataset.")
    else:
        df = df_active

# ── 3. KPIs ────────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Jobs", len(df))
with col2:
    st.metric("Countries", df['country'].nunique() if 'country' in df.columns else 0)
with col3:
    st.metric("Companies", df['company_name'].nunique() if 'company_name' in df.columns else 0)
with col4:
    avg = df[df['salary_min'] > 0]['salary_min'].mean()
    st.metric("Avg Salary (USD)", f"${avg:,.0f}" if pd.notna(avg) else "N/A")

st.divider()

# ── 4. GRÁFICOS ────────────────────────────────────────────────────────────────
if not df.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Jobs by Country")
        st.bar_chart(df['country'].value_counts(), color="#3d85c6")

    with col2:
        st.subheader("Seniority Distribution")
        st.bar_chart(df['seniority'].value_counts(), color="#6fa8dc")

    # Tabela salarial dinâmica
    st.subheader("Average Salary by Country & Seniority")
    if df['salary_min'].gt(0).any():
        salary_table = (
            df[df['salary_min'] > 0]
            .groupby(['country', 'seniority'])['salary_min']
            .mean()
            .reset_index()
            .sort_values('salary_min', ascending=False)
        )
        st.dataframe(
            salary_table,
            column_config={
                "salary_min": st.column_config.NumberColumn("Avg Salary (USD)", format="$%.0f")
            },
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No salary data available for this period.")

st.divider()

# ── 5. TABELA DE VAGAS ─────────────────────────────────────────────────────────
st.subheader("Latest Job Postings")
st.caption(f"Showing {len(df)} active jobs posted in the last 28 days")

all_cols   = ['title', 'company_name', 'country', 'seniority',
              'salary_min', 'salary_max', 'date_posted', 'redirect_url']
show_cols  = [c for c in all_cols if c in df.columns]

col_config = {
    "title":        st.column_config.TextColumn("Job Title",    width="large"),
    "company_name": st.column_config.TextColumn("Company",      width="medium"),
    "country":      st.column_config.TextColumn("Country",      width="small"),
    "seniority":    st.column_config.TextColumn("Level",        width="small"),
    "salary_min":   st.column_config.NumberColumn("Salary Min", format="$%.0f"),
    "salary_max":   st.column_config.NumberColumn("Salary Max", format="$%.0f"),
    "date_posted":  st.column_config.DateColumn("Posted On",    format="DD/MM/YYYY"),
    "redirect_url": st.column_config.LinkColumn("Apply"),
}

st.data_editor(
    df[show_cols].sort_values('date_posted', ascending=False).head(50),
    column_config=col_config,
    hide_index=True,
    use_container_width=True,
    disabled=True
)