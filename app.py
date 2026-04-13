import streamlit as st
import pandas as pd
import re
import unicodedata
import hashlib
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG & CONSTANTS [cite: 1, 2]
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Buscador de Talentos", page_icon="🔍", layout="wide")

TABLE = "leads"
BATCH_SIZE = 500 [cite: 2]

COLUNAS_BANCO = [
    'name', 'cargo', 'occupation', 'job_title', 'company_name',
    'expertise', 'nivel_senioridade', 'senioridade_normalizada',
    'segmento_empresa', 'segmento_mercado', 'linkedin_email',
    'ddd_telefone', 'linkedin_url', 'cidade', 'uf', 'estado', 'pais',
    'publico_alvo_ads', 'recrutador', 'sales_navigator_id',
    'senioridade_aproximada',
] [cite: 2]

COL_MAP = {
    'name': 'name', 'Cargo': 'cargo', 'occupation': 'occupation',
    'job_title': 'job_title', 'company_name': 'company_name',
    'Expertise': 'expertise', 'Nivel_Senioridade': 'nivel_senioridade',
    'senioridade_normalizada': 'senioridade_normalizada',
    'Segmento_Empresa': 'segmento_empresa', 'segmento_mercado': 'segmento_mercado',
    'linkedinEmail': 'linkedin_email', 'DDD_Telefone': 'ddd_telefone',
    'linkedinUrl': 'linkedin_url', 'Cidade': 'cidade', 'UF': 'uf',
    'Estado': 'estado', 'Pais': 'pais', 'Publico_Alvo_Ads': 'publico_alvo_ads',
    'Recrutador?': 'recrutador', 'salesNavigatorId': 'sales_navigator_id',
    'senioridade_aproximada': 'senioridade_aproximada',
} [cite: 2]

# ══════════════════════════════════════════════════════════════════════════════
# CONEXÃO SUPABASE [cite: 3]
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
    except Exception:
        return 0 [cite: 11]

# ══════════════════════════════════════════════════════════════════════════════
# LÓGICA DE FILTROS E BUSCA [cite: 12, 13, 14]
# ══════════════════════════════════════════════════════════════════════════════
def get_filter_options():
    sb = get_supabase()
    options = {}
    for col in ['senioridade_normalizada', 'expertise', 'segmento_empresa', 'segmento_mercado']:
        try:
            # Tenta via RPC (mais rápido)
            result = sb.rpc("get_distinct_values", {"col_name": col}).execute()
            if result.data:
                options[col] = sorted([r['val'] for r in result.data if r['val'] and r['val'] not in ('nan', '', 'None')])
            else:
                options[col] = []
        except Exception:
            # Fallback direto se o RPC falhar [cite: 13]
            try:
                result = sb.table(TABLE).select(col).limit(1000).execute()
                vals = set(r[col] for r in result.data if r.get(col) and r[col] not in ('nan', '', 'None'))
                options[col] = sorted(vals)
            except Exception:
                options[col] = []
    return options

def search_leads(query="", filters=None, limit=50):
    sb = get_supabase()
    q = sb.table(TABLE).select("*")
    if filters:
        if filters.get('senioridade_normalizada'):
            q = q.in_("senioridade_normalizada", filters['senioridade_normalizada'])
        if filters.get('expertise'):
            or_parts = ",".join([f"expertise.ilike.%{e}%" for e in filters['expertise']])
            q = q.or_(or_parts)
        if filters.get('segmento_empresa'):
            q = q.in_("segmento_empresa", filters['segmento_empresa'])
        if filters.get('segmento_mercado'):
            q = q.in_("segmento_mercado", filters['segmento_mercado'])
    
    if query.strip():
        term = query.strip()
        q = q.or_(f"name.ilike.%{term}%,cargo.ilike.%{term}%,company_name.ilike.%{term}%")

    result = q.limit(limit).execute() [cite: 19]
    return result.data or []

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE DE TRATAMENTO [cite: 24, 25, 29, 43]
# ══════════════════════════════════════════════════════════════════════════════
def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', str(s)) if unicodedata.category(c) != 'Mn').lower().strip()

def _norm_linkedin(url):
    if pd.isna(url): return None
    url = str(url).strip().lower().split('?')[0].rstrip('/')
    if '/in/' in url:
        return url.split('/in/')[-1].rstrip('/')
    return None

def limpar_localizacao(df):
    df = df.copy()
    for col in ['Cidade', 'UF', 'Estado', 'Pais']:
        if col not in df.columns: df[col] = ''
        df[col] = df[col].astype(str).replace('nan', '')
    return df [cite: 25]

def normalizar_senioridade(df):
    # Implementação simplificada para o pipeline
    df['senioridade_normalizada'] = 'Nao Identificado'
    df['senioridade_aproximada'] = False
    return df [cite: 40]

def deduplicar_local(df):
    df['_linkedin_slug'] = df['linkedinUrl'].apply(_norm_linkedin)
    antes = len(df)
    df = df.drop_duplicates(subset=['_linkedin_slug']).copy() if '_linkedin_slug' in df.columns else df
    return df, antes, len(df) [cite: 43]

# ══════════════════════════════════════════════════════════════════════════════
# SALVAMENTO EM LOTE [cite: 20, 21]
# ══════════════════════════════════════════════════════════════════════════════
def insert_leads_batch(records, progress_callback=None):
    sb = get_supabase()
    total = len(records)
    inserted = 0
    for i in range(0, total, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            sb.table(TABLE).insert(batch).execute()
            inserted += len(batch)
            if progress_callback:
                progress_callback(75 + int((i/total)*25), f"📤 Enviando {inserted}/{total}...")
        except Exception as e:
            if 'duplicate' in str(e).lower():
                for r in batch:
                    try: sb.table(TABLE).insert(r).execute(); inserted += 1
                    except: pass
    return inserted [cite: 20]

def pipeline_e_salva(uploaded_file, progress_callback=None):
    if progress_callback: progress_callback(10, "📂 Lendo arquivo...")
    df = pd.read_csv(uploaded_file, dtype=str) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file, dtype=str)
    
    df = limpar_localizacao(df)
    df = normalizar_senioridade(df)
    df, antes, depois = deduplicar_local(df)
    
    df_out = pd.DataFrame()
    for orig, dest in COL_MAP.items():
        df_out[dest] = df[orig] if orig in df.columns else None
    
    df_out['linkedin_slug'] = df['_linkedin_slug']
    records = df_out.to_dict(orient='records')
    
    # Limpeza final de nans para o Supabase
    for r in records:
        for k,v in r.items():
            if pd.isna(v) or v in ('nan', 'None', ''): r[k] = None

    inserted = insert_leads_batch(records, progress_callback)
    return {'arquivo': uploaded_file.name, 'linhas_raw': len(df), 'depois_dedup': depois, 'inseridos': inserted} [cite: 52]

# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE [cite: 53, 63, 66]
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""<style>
    .metric-card { background: #1e293b; padding: 1rem; border-radius: 10px; text-align: center; border: 1px solid #475569; }
    .lead-card { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem; color: #1e293b; }
</style>""", unsafe_allow_html=True) [cite: 53]

with st.sidebar:
    st.header("📤 Importar Data")
    total_banco = count_leads()
    st.metric("Leads no Banco", f"{total_banco:,}") [cite: 62]
    
    uploaded = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'])
    if uploaded and st.button("🚀 Processar Agora", use_container_width=True):
        prog = st.progress(0)
        msg = st.empty()
        def up(p, m): prog.progress(p/100); msg.text(m)
        try:
            res = pipeline_e_salva(uploaded, up)
            st.success(f"Feito! {res['inseridos']} novos leads.")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"Erro: {e}")

# ── CONTEÚDO PRINCIPAL ────────────────────────────────────────────────────────
st.title("🔍 Buscador de Talentos") [cite: 66]

options = get_filter_options()
c1, c2, c3 = st.columns(3)
with c1: f_sen = st.multiselect("Senioridade", options.get('senioridade_normalizada', []))
with c2: f_exp = st.multiselect("Expertise", options.get('expertise', []))
with c3: f_seg = st.multiselect("Segmento", options.get('segmento_empresa', []))

busca = st.text_input("Busca livre (Nome, Cargo, Empresa)")

if busca or f_sen or f_exp or f_seg:
    filtros = {'senioridade_normalizada': f_sen, 'expertise': f_exp, 'segmento_empresa': f_seg}
    resultados = search_leads(busca, filtros)
    st.write(f"Exibindo {len(resultados)} resultados")
    
    for r in resultados:
        with st.container():
            st.markdown(f"""<div class="lead-card">
                <strong>{r.get('name') or '—'}</strong> • {r.get('cargo') or '—'}<br>
                <small>{r.get('company_name') or '—'} | {r.get('cidade') or ''}-{r.get('uf') or ''}</small>
            </div>""", unsafe_allow_html=True) [cite: 72]
else:
    st.info("Aguardando filtros ou termo de busca...")
