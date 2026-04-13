import streamlit as st
import pandas as pd
import re
import unicodedata
import hashlib
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES INICIAIS
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Buscador de Talentos", page_icon="🔍", layout="wide")

TABLE = "leads"
BATCH_SIZE = 500

# Mapeia as colunas da sua planilha para as colunas do banco de dados
COL_MAP = {
    'name': 'name', 
    'Cargo': 'cargo', 
    'occupation': 'occupation',
    'job_title': 'job_title', 
    'company_name': 'company_name',
    'Expertise': 'expertise', 
    'Nivel_Senioridade': 'nivel_senioridade',
    'senioridade_normalizada': 'senioridade_normalizada',
    'Segmento_Empresa': 'segmento_empresa', 
    'segmento_mercado': 'segmento_mercado',
    'linkedinEmail': 'linkedin_email', 
    'DDD_Telefone': 'ddd_telefone',
    'linkedinUrl': 'linkedin_url', 
    'Cidade': 'cidade', 
    'UF': 'uf',
    'Estado': 'estado', 
    'Pais': 'pais', 
    'Publico_Alvo_Ads': 'publico_alvo_ads',
    'Recrutador?': 'recrutador', 
    'salesNavigatorId': 'sales_navigator_id',
    'senioridade_aproximada': 'senioridade_aproximada',
}

# ══════════════════════════════════════════════════════════════════════════════
# FUNÇÕES DE BANCO DE DADOS
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

def count_leads():
    try:
        sb = get_supabase()
        result = sb.table(TABLE).select("id", count="exact").limit(1).execute()
        return result.count or 0
    except:
        return 0

def get_filter_options():
    sb = get_supabase()
    options = {}
    cols = ['senioridade_normalizada', 'expertise', 'segmento_empresa']
    for col in cols:
        try:
            # Tenta usar a função RPC que criamos no SQL Editor
            result = sb.rpc("get_distinct_values", {"col_name": col}).execute()
            if result.data:
                options[col] = sorted([r['val'] for r in result.data if r['val']])
            else:
                options[col] = []
        except:
            options[col] = []
    return options

# ══════════════════════════════════════════════════════════════════════════════
# PROCESSAMENTO DE DADOS (PIPELINE)
# ══════════════════════════════════════════════════════════════════════════════
def extrair_slug(url):
    if pd.isna(url) or not isinstance(url, str): return None
    url = url.strip().lower().split('?')[0].rstrip('/')
    if '/in/' in url:
        return url.split('/in/')[-1]
    return hashlib.md5(url.encode()).hexdigest()

def pipeline_e_salva(uploaded_file, progress_cb):
    sb = get_supabase()
    progress_cb(10, "Lendo arquivo...")
    
    if uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file, dtype=str)
    else:
        df = pd.read_excel(uploaded_file, dtype=str)

    progress_cb(30, "Limpando e deduplicando...")
    # Criar slug para evitar duplicatas
    df['linkedin_slug'] = df['linkedinUrl'].apply(extrair_slug)
    df = df.drop_duplicates(subset=['linkedin_slug'])

    # Mapeamento de colunas
    final_records = []
    for _, row in df.iterrows():
        record = {}
        for csv_col, db_col in COL_MAP.items():
            val = row.get(csv_col)
            record[db_col] = val if pd.notna(val) and val != 'nan' else None
        
        record['linkedin_slug'] = row.get('linkedin_slug')
        final_records.append(record)

    progress_cb(60, f"Enviando {len(final_records)} leads...")
    
    sucesso = 0
    for i in range(0, len(final_records), BATCH_SIZE):
        batch = final_records[i:i+BATCH_SIZE]
        try:
            sb.table(TABLE).insert(batch).execute()
            sucesso += len(batch)
        except Exception as e:
            # Se falhar o lote (ex: uma duplicata), tenta um por um
            for r in batch:
                try:
                    sb.table(TABLE).insert(r).execute()
                    sucesso += 1
                except:
                    continue
        
        pct = 60 + int((i / len(final_records)) * 40)
        progress_cb(pct, f"Enviando... {sucesso} processados")

    return sucesso

# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("📤 Importação")
    
    qtd = count_leads()
    st.metric("Total no Banco", f"{qtd:,}")
    
    st.markdown("---")
    file = st.file_uploader("Suba sua planilha (CSV ou XLSX)", type=['csv', 'xlsx'])
    
    if file and st.button("🚀 Iniciar Processamento", use_container_width=True):
        bar = st.progress(0)
        status = st.empty()
        
        def update(p, m):
            bar.progress(p/100)
            status.text(m)
            
        try:
            total = pipeline_e_salva(file, update)
            st.success(f"Finalizado! {total} leads processados/atualizados.")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"Erro crítico: {e}")

# ÁREA PRINCIPAL
st.title("🔍 Buscador de Talentos")

opts = get_filter_options()

c1, c2 = st.columns(2)
with c1:
    f_sen = st.multiselect("Senioridade", opts.get('senioridade_normalizada', []))
with c2:
    f_exp = st.multiselect("Expertise", opts.get('expertise', []))

busca = st.text_input("Buscar por Nome, Cargo ou Empresa")

if st.button("Buscar"):
    sb = get_supabase()
    query = sb.table(TABLE).select("*")
    
    if f_sen: query = query.in_("senioridade_normalizada", f_sen)
    if f_exp: query = query.in_("expertise", f_exp)
    if busca: query = query.or_(f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%")
    
    res = query.limit(100).execute()
    
    if res.data:
        st.write(f"Encontrados {len(res.data)} leads:")
        for r in res.data:
            with st.expander(f"{r['name']} - {r['cargo']} @ {r['company_name']}"):
                st.write(f"**Email:** {r['linkedin_email']}")
                st.write(f"**Local:** {r['cidade']} / {r['uf']}")
                st.write(f"**LinkedIn:** {r['linkedin_url']}")
    else:
        st.warning("Nenhum lead encontrado com esses filtros.")
