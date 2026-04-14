"""
Buscador de Talentos V2 - Streamlit + Supabase
Foco em UX/UI Moderno, Organização por Abas e Design System Aprimorado.
"""
import streamlit as st
import pandas as pd
import re
import unicodedata
import hashlib
from supabase import create_client

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Buscador de Talentos Pro",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CONSTANTES ---
TABLE = "leads"
BATCH_SIZE = 500
COLUNAS_BANCO = [
    'name', 'cargo', 'occupation', 'job_title', 'company_name',
    'expertise', 'nivel_senioridade', 'senioridade_normalizada',
    'segmento_empresa', 'segmento_mercado', 'linkedin_email',
    'ddd_telefone', 'linkedin_url', 'cidade', 'uf', 'estado', 'pais',
    'publico_alvo_ads', 'recrutador', 'sales_navigator_id',
    'senioridade_aproximada',
]

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
    'Categoria_Cargo': 'categoria_cargo',
}

# --- DESIGN SYSTEM (CSS) ---
CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

:root {
    --primary: #6366f1;
    --primary-hover: #4f46e5;
    --bg-main: #f8fafc;
    --card-bg: #ffffff;
    --text-main: #1e293b;
    --text-muted: #64748b;
    --border: #e2e8f0;
    --radius: 12px;
}

.main .block-container { 
    max-width: 1400px; 
    padding-top: 2rem; 
    background-color: var(--bg-main);
}

* {
    font-family: 'Inter', sans-serif !important;
}

/* Header Styling */
.header-container {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-bottom: 2rem;
    padding: 1.5rem;
    background: white;
    border-radius: var(--radius);
    border: 1px solid var(--border);
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

.header-icon {
    width: 48px;
    height: 48px;
    background: var(--primary);
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
    font-size: 1.5rem;
    font-weight: 800;
}

.header-text h1 {
    font-size: 1.75rem;
    font-weight: 700;
    color: var(--text-main);
    margin: 0;
}

.header-text p {
    font-size: 0.9rem;
    color: var(--text-muted);
    margin: 0;
}

/* KPI Cards */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
}

.kpi-card {
    background: white;
    padding: 1.25rem;
    border-radius: var(--radius);
    border: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
}

.kpi-label {
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
}

.kpi-value {
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--text-main);
}

/* Search & Filters */
.search-section {
    background: white;
    padding: 1.5rem;
    border-radius: var(--radius);
    border: 1px solid var(--border);
    margin-bottom: 2rem;
}

/* Result Cards */
.results-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
    gap: 1.25rem;
}

.talent-card {
    background: white;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem;
    transition: all 0.2s ease;
    display: flex;
    flex-direction: column;
    height: 100%;
}

.talent-card:hover {
    border-color: var(--primary);
    box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
    transform: translateY(-2px);
}

.talent-header {
    display: flex;
    gap: 1rem;
    margin-bottom: 1rem;
}

.talent-avatar {
    width: 56px;
    height: 56px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
    font-weight: 700;
    font-size: 1.2rem;
    flex-shrink: 0;
}

.talent-info {
    flex: 1;
    min-width: 0;
}

.talent-name {
    font-size: 1.1rem;
    font-weight: 700;
    color: var(--text-main);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.talent-role {
    font-size: 0.9rem;
    color: var(--text-muted);
    margin-top: 2px;
    line-height: 1.2;
}

.talent-company {
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--primary);
    margin-top: 4px;
}

.talent-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 1rem;
}

.badge {
    padding: 4px 10px;
    border-radius: 6px;
    font-size: 0.7rem;
    font-weight: 600;
}

.badge-seniority { background: #eef2ff; color: #4f46e5; }
.badge-location { background: #f0fdf4; color: #166534; }
.badge-expertise { background: #fff7ed; color: #9a3412; }
.badge-recruiter { background: #fef2f2; color: #991b1b; }

.talent-footer {
    margin-top: auto;
    padding-top: 1rem;
    border-top: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.contact-links {
    display: flex;
    gap: 12px;
}

.contact-link {
    color: var(--text-muted);
    text-decoration: none;
    font-size: 0.8rem;
    transition: color 0.2s;
}

.contact-link:hover {
    color: var(--primary);
}

/* Empty State */
.empty-state {
    text-align: center;
    padding: 4rem 2rem;
    background: white;
    border-radius: var(--radius);
    border: 2px dashed var(--border);
}

.empty-state h3 { color: var(--text-main); margin-bottom: 0.5rem; }
.empty-state p { color: var(--text-muted); }

/* Tabs Styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 24px;
    background-color: transparent;
}

.stTabs [data-baseweb="tab"] {
    height: 50px;
    white-space: pre-wrap;
    background-color: transparent;
    border-radius: 4px 4px 0px 0px;
    gap: 1px;
    padding-top: 10px;
    padding-bottom: 10px;
}

.stTabs [aria-selected="true"] {
    background-color: transparent;
    border-bottom: 2px solid var(--primary) !important;
    color: var(--primary) !important;
}
</style>
"""

# --- SUPABASE CLIENT ---
@st.cache_resource
def get_supabase():
    try:
        return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    except Exception as e:
        st.error(f"Erro ao conectar ao Supabase: {e}")
        return None

# --- FUNÇÕES DE DADOS ---
def count_leads():
    sb = get_supabase()
    if not sb: return 0
    try:
        result = sb.table(TABLE).select("id", count="exact").limit(1).execute()
        return result.count or 0
    except Exception:
        return 0

@st.cache_data(ttl=600)
def get_filter_options():
    sb = get_supabase()
    if not sb: return {}
    options = {}
    cols = ['senioridade_normalizada', 'expertise', 'segmento_empresa', 'segmento_mercado']
    for col in cols:
        try:
            result = sb.rpc("get_distinct_values", {"col_name": col}).execute()
            if result.data:
                options[col] = sorted([r['val'] for r in result.data if r['val'] and r['val'] not in ('nan', '', 'None')])
            else:
                result = sb.table(TABLE).select(col).limit(5000).execute()
                vals = set(r[col] for r in result.data if r.get(col) and r[col] not in ('nan', '', 'None'))
                options[col] = sorted(vals)
        except Exception:
            options[col] = []
    return options

def search_leads(query="", filters=None, limit=50):
    sb = get_supabase()
    if not sb: return []
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
        if filters.get('estados'):
            or_parts = ",".join(
                [f"uf.ilike.{e}" for e in filters['estados']] +
                [f"estado.ilike.{e}" for e in filters['estados']])
            q = q.or_(or_parts)
            
    if query.strip():
        terms = [t for t in query.lower().split() if len(t) > 2]
        for term in terms:
            or_parts = ",".join([
                f"cargo.ilike.%{term}%", f"occupation.ilike.%{term}%",
                f"job_title.ilike.%{term}%", f"expertise.ilike.%{term}%",
                f"company_name.ilike.%{term}%", f"name.ilike.%{term}%",
                f"cidade.ilike.%{term}%", f"uf.ilike.%{term}%",
                f"estado.ilike.%{term}%", f"segmento_mercado.ilike.%{term}%"])
            q = q.or_(or_parts)
            
    try:
        result = q.limit(limit).execute()
        return result.data or []
    except Exception as e:
        st.error(f"Erro na busca: {e}")
        return []

# --- PIPELINE DE LIMPEZA & NORMALIZAÇÃO (Original Logic) ---
def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def _norm_str(s):
    if pd.isna(s): return None
    s = str(s).strip().lower()
    return s if s not in ('', 'nan', 'none', 'n/a') else None

def _norm_linkedin(url):
    if pd.isna(url): return None
    url = str(url).strip().lower().split('?')[0].rstrip('/')
    if '/in/' in url:
        slug = url.split('/in/')[-1].rstrip('/')
        return slug if slug else None
    return None

def _safe_clean(val):
    if val is None: return None
    try:
        if pd.isna(val): return None
    except (TypeError, ValueError): pass
    s = str(val).strip()
    if s.lower() in ('nan', 'none', '', 'nat', 'n/a'): return None
    return s

def limpar_localizacao(df):
    df = df.copy()
    for col in ['Cidade', 'UF', 'Estado', 'Pais', 'location']:
        if col not in df.columns: df[col] = ''
        df[col] = df[col].astype(str).replace('nan', '')
    uf_map = {'Federal District': 'Brasilia', 'Brasilia': 'Brasilia'}
    for old, new in uf_map.items(): df.loc[df['UF'] == old, 'UF'] = new
    cidade_map = {'Federal District': 'Brasilia', 'Distrito Federal': 'Brasilia', 'Brasilia': 'Brasilia'}
    for old, new in cidade_map.items(): df.loc[df['Cidade'] == old, 'Cidade'] = new
    for cidade, uf in {'Planaltina': 'Distrito Federal', 'Brasilia': 'Distrito Federal', 'Taguatinga': 'Distrito Federal', 'Ceilandia': 'Distrito Federal', 'Gama': 'Distrito Federal'}.items():
        df.loc[df['Cidade'] == cidade, 'UF'] = uf
    for cidade, estado in {'Sao Paulo': 'Sao Paulo', 'Osasco': 'Sao Paulo', 'Guarulhos': 'Sao Paulo', 'San Pablo': 'Filipinas'}.items():
        df.loc[df['Cidade'] == cidade, 'Estado'] = estado
    for old, new in {'San Pablo': 'Sao Paulo', 'Sao Paulo': 'SP', 'Brasil': 'DF', 'Minnesota': 'EUA'}.items():
        df.loc[df['Estado'] == old, 'Estado'] = new
    df.loc[df['UF'] == 'San Pablo', 'Estado'] = 'Filipinas'
    df.loc[df['UF'] == 'Distrito Federal', 'Estado'] = 'DF'
    df.loc[df['location'] == 'Brasil', 'Pais'] = 'Brasil'
    df.loc[df['location'] == 'Brazil', 'location'] = 'Brasil'
    mask_exterior = (df['Pais'].str.strip() != '') & (df['Pais'] != 'Brasil') & (df['Pais'] != 'nan')
    df.loc[mask_exterior, 'Estado'] = 'Exterior'
    return df

CAT18_SEN_MAP = {
    'Lideranca/Executivo': 'Lideranca/Executivo', 'Lideranca': 'Lideranca/Executivo',
    'Senior/Especialista': 'Senior/Especialista', 'Senior': 'Senior/Especialista',
    'Especialista': 'Senior/Especialista', 'Pleno': 'Pleno',
    'Junior': 'Junior/Estagio', 'Junior/Trainee': 'Junior/Estagio',
    'Estagio/Trainee': 'Junior/Estagio', 'Estagiario': 'Junior/Estagio',
    'Coordenacao': 'Lideranca/Executivo', 'Gerencia': 'Lideranca/Executivo',
}

def _inferir_exact(nivel, cargo, jt, cat18):
    n = strip_accents(str(nivel).lower().strip())
    if any(x in n for x in ('estagio','trainee','estagiario','aprendiz')): return 'Junior/Estagio'
    if 'junior' in n: return 'Junior/Estagio'
    if 'pleno' in n: return 'Pleno'
    if any(x in n for x in ('senior','especialista')): return 'Senior/Especialista'
    if any(x in n for x in ('lideranca','executivo','coordenacao','gerencia')): return 'Lideranca/Executivo'
    jt_first = str(jt).split('|')[0] if pd.notna(jt) else ''
    texto = strip_accents((str(cargo) + ' ' + jt_first).lower())
    if re.search(r'(estagio|estagiario|trainee|aprendiz|intern\b|apprentice)', texto): return 'Junior/Estagio'
    if re.search(r'(\bjunior\b|\bjr\b)', texto): return 'Junior/Estagio'
    if re.search(r'\bpleno\b', texto): return 'Pleno'
    if re.search(r'(\bsenior\b|\bsr\b|\bespecialista\b|\blead\b|\bstaff\b|\bprincipal\b)', texto): return 'Senior/Especialista'
    if re.search(r'(diretor|diretora|ceo|cto|cfo|coo|cio|chro|ciso|cmo|head |founder|co-founder|gerente|manager|coordenador|supervisor|\bvp\b|vice.president|managing|presidente)', texto): return 'Lideranca/Executivo'
    return 'Nao Identificado'

def normalizar_senioridade(df):
    for col in ['Nivel_Senioridade', 'Cargo', 'job_title', 'Categoria_Cargo', 'segmento_mercado', 'Expertise']:
        if col not in df.columns: df[col] = ''
    df['senioridade_normalizada'] = df.apply(lambda r: _inferir_exact(r.get('Nivel_Senioridade', ''), r.get('Cargo', ''), r.get('job_title', ''), r.get('Categoria_Cargo', '')), axis=1)
    df['senioridade_aproximada'] = False
    return df

def deduplicar_local(df):
    for col in ['salesNavigatorId', 'linkedinUrl', 'linkedinEmail']:
        if col not in df.columns: df[col] = None
    df['_salesnavid_norm'] = df['salesNavigatorId'].apply(_norm_str)
    df['_linkedin_slug'] = df['linkedinUrl'].apply(_norm_linkedin)
    df['_email_norm'] = df['linkedinEmail'].apply(_norm_str)
    df['_score'] = df.notna().sum(axis=1)
    df = df.sort_values('_score', ascending=False)
    mask = df['_salesnavid_norm'].notna()
    df = df[~(mask & df.duplicated(subset=['_salesnavid_norm'], keep='first'))].copy()
    mask = df['_linkedin_slug'].notna()
    df = df[~(mask & df.duplicated(subset=['_linkedin_slug'], keep='first'))].copy()
    mask = df['_email_norm'].notna()
    df = df[~(mask & df.duplicated(subset=['_email_norm'], keep='first'))].copy()
    df = df.drop(columns=['_salesnavid_norm', '_email_norm', '_score'])
    return df

def insert_leads_batch(records, progress_callback=None):
    sb = get_supabase()
    if not sb: return 0
    total = len(records)
    inserted = 0
    for i in range(0, total, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            sb.table(TABLE).insert(batch).execute()
            inserted += len(batch)
            if progress_callback:
                pct = int((inserted / total) * 100)
                progress_callback(pct, f"Enviando... {inserted:,}/{total:,}")
        except Exception as e:
            for record in batch:
                try:
                    sb.table(TABLE).insert(record).execute()
                    inserted += 1
                except: pass
    return inserted

def pipeline_e_salva(uploaded_file, progress_callback=None):
    if progress_callback: progress_callback(5, "Lendo arquivo...")
    name = uploaded_file.name.lower()
    if name.endswith('.csv'):
        df = pd.read_csv(uploaded_file, dtype=str)
    else:
        df = pd.read_excel(uploaded_file, dtype=str)
    
    total_raw = len(df)
    if progress_callback: progress_callback(20, "Limpando localizacao...")
    df = limpar_localizacao(df)
    if progress_callback: progress_callback(40, "Normalizando senioridade...")
    df = normalizar_senioridade(df)
    if progress_callback: progress_callback(60, "Deduplicando...")
    df = deduplicar_local(df)
    
    df_out = pd.DataFrame()
    for orig, dest in COL_MAP.items():
        if orig in df.columns: df_out[dest] = df[orig].apply(_safe_clean)
        else: df_out[dest] = None
    
    # Gerar slugs
    slugs = []
    for idx, row in df.iterrows():
        slug = _norm_linkedin(row.get('linkedinUrl'))
        if not slug:
            email = _norm_str(row.get('linkedinEmail', ''))
            slug = f"email:{email}" if email else f"hash:{hashlib.md5(str(idx).encode()).hexdigest()[:16]}"
        slugs.append(slug)
    df_out['linkedin_slug'] = slugs
    
    records = df_out.to_dict(orient='records')
    inserted = insert_leads_batch(records, progress_callback)
    return {'arquivo': uploaded_file.name, 'linhas_raw': total_raw, 'inseridos': inserted}

# --- COMPONENTES DE UI ---
def render_header():
    st.markdown(f"""
    <div class="header-container">
        <div class="header-icon">BT</div>
        <div class="header-text">
            <h1>Buscador de Talentos</h1>
            <p>Encontre os melhores profissionais com filtros inteligentes e busca semântica.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_kpis(total_leads):
    st.markdown(f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-label">Total de Leads</div>
            <div class="kpi-value">{total_leads:,}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Status do Banco</div>
            <div class="kpi-value" style="color: #10b981;">Online</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Filtros Ativos</div>
            <div class="kpi-value" style="font-size: 1rem;">Inteligência de Dados</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def _avatar_color(name):
    colors = ['#6366f1', '#8b5cf6', '#ec4899', '#f43f5e', '#f59e0b', '#10b981', '#06b6d4']
    h = sum(ord(c) for c in (name or '?'))
    return colors[h % len(colors)]

def _initials(name):
    if not name or name == '-': return '?'
    parts = name.strip().split()
    if len(parts) >= 2: return (parts[0][0] + parts[-1][0]).upper()
    return parts[0][0].upper()

def render_talent_card(r):
    nome = r.get('name') or 'Profissional'
    cargo = r.get('cargo') or r.get('occupation') or 'Cargo não informado'
    company = r.get('company_name') or ''
    sen = r.get('senioridade_normalizada') or ''
    expertise = r.get('expertise') or ''
    loc = f"{r.get('cidade', '')} {r.get('uf', '')}".strip()
    li_url = r.get('linkedin_url') or '#'
    email = r.get('linkedin_email') or ''
    
    acolor = _avatar_color(nome)
    ini = _initials(nome)
    
    tags_html = ""
    if sen and sen != 'Nao Identificado': tags_html += f'<span class="badge badge-seniority">{sen}</span>'
    if loc: tags_html += f'<span class="badge badge-location">{loc}</span>'
    if expertise: tags_html += f'<span class="badge badge-expertise">{expertise[:25]}</span>'
    if str(r.get('recrutador')).lower() in ('sim', 'true', '1'):
        tags_html += f'<span class="badge badge-recruiter">Recrutador</span>'

    return f"""
    <div class="talent-card">
        <div class="talent-header">
            <div class="talent-avatar" style="background: {acolor}">{ini}</div>
            <div class="talent-info">
                <div class="talent-name">{nome}</div>
                <div class="talent-role">{cargo}</div>
                <div class="talent-company">{company}</div>
            </div>
        </div>
        <div class="talent-tags">
            {tags_html}
        </div>
        <div class="talent-footer">
            <div class="contact-links">
                {"<a href='mailto:"+email+"' class='contact-link'>Email</a>" if email else ""}
                <a href="{li_url}" target="_blank" class="contact-link">LinkedIn</a>
            </div>
            <div style="font-size: 0.7rem; color: #94a3b8;">ID: {str(r.get('id'))[:8]}</div>
        </div>
    </div>
    """

# --- APP PRINCIPAL ---
def main():
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    render_header()
    
    total = count_leads()
    
    tab_search, tab_import, tab_admin = st.tabs([
        "🔍 Buscar Talentos", 
        "📥 Importar Dados", 
        "⚙️ Manutenção"
    ])
    
    # --- ABA BUSCA ---
    with tab_search:
        render_kpis(total)
        
        with st.container():
            st.markdown('<div class="search-section">', unsafe_allow_html=True)
            c1, c2 = st.columns([3, 1])
            with c1:
                query = st.text_input("O que você procura?", placeholder="Ex: Desenvolvedor Senior em São Paulo, Especialista em Dados...")
            with c2:
                limit = st.selectbox("Resultados", [50, 100, 500, 1000], index=0)
            
            with st.expander("🎯 Filtros Avançados", expanded=False):
                options = get_filter_options()
                f1, f2, f3 = st.columns(3)
                with f1: sen_f = st.multiselect("Senioridade", options.get('senioridade_normalizada', []))
                with f2: exp_f = st.multiselect("Expertise", options.get('expertise', []))
                with f3: seg_f = st.multiselect("Segmento", options.get('segmento_empresa', []))
            st.markdown('</div>', unsafe_allow_html=True)

        if query or sen_f or exp_f or seg_f:
            filters = {}
            if sen_f: filters['senioridade_normalizada'] = sen_f
            if exp_f: filters['expertise'] = exp_f
            if seg_f: filters['segmento_empresa'] = seg_f
            
            with st.spinner("Buscando talentos..."):
                results = search_leads(query, filters, limit)
            
            if results:
                st.subheader(f"🎯 {len(results)} profissionais encontrados")
                
                ac1, ac2, _ = st.columns([1, 1, 4])
                df_res = pd.DataFrame(results)
                with ac1:
                    st.download_button("📥 Exportar CSV", df_res.to_csv(index=False).encode('utf-8'), "talentos.csv", "text/csv", use_container_width=True)
                with ac2:
                    emails = [r.get('linkedin_email') for r in results if r.get('linkedin_email')]
                    if emails:
                        st.download_button("📧 Lista de Emails", "\n".join(emails), "emails.txt", "text/plain", use_container_width=True)
                
                cards_html = "".join([render_talent_card(r) for r in results])
                st.markdown(f'<div class="results-grid">{cards_html}</div>', unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="empty-state">
                    <h3>Nenhum resultado encontrado</h3>
                    <p>Tente ajustar seus filtros ou usar termos mais genéricos na busca.</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="empty-state">
                <h3>Pronto para começar?</h3>
                <p>Digite um cargo, tecnologia ou use os filtros acima para explorar o banco de talentos.</p>
            </div>
            """, unsafe_allow_html=True)

    # --- ABA IMPORTAÇÃO ---
    with tab_import:
        st.subheader("📥 Importar Nova Planilha")
        st.info("O sistema aceita arquivos CSV ou XLSX. Os dados passarão por limpeza, normalização de senioridade e deduplicação automática.")
        
        uploaded = st.file_uploader("Arraste seu arquivo aqui", type=['csv', 'xlsx'])
        if uploaded:
            if st.button("🚀 Iniciar Processamento", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_prog(p, m):
                    progress_bar.progress(p/100)
                    status_text.text(m)
                
                try:
                    res = pipeline_e_salva(uploaded, update_prog)
                    st.success(f"Importação concluída! {res['inseridos']:,} leads salvos.")
                    st.balloons()
                except Exception as e:
                    st.error(f"Erro no processamento: {e}")

    # --- ABA ADMIN ---
    with tab_admin:
        st.subheader("⚙️ Manutenção do Sistema")
        c1, c2 = st.columns(2)
        with c1:
            st.write("### Estatísticas")
            st.metric("Total de Leads", f"{total:,}")
        
        with c2:
            st.write("### Ações Críticas")
            if st.button("⚠️ LIMPAR TODO O BANCO", use_container_width=True):
                sb = get_supabase()
                try:
                    sb.table(TABLE).delete().neq("id", 0).execute()
                    st.success("Banco limpo com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao limpar banco: {e}")

if __name__ == "__main__":
    main()
