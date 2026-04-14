"""
Buscador de Talentos - Talent Analytics Pro
Pipeline: Upload CSV/XLSX → Limpeza → Normalização → Deduplicação → Supabase → Busca
"""
import streamlit as st
import pandas as pd
import re
import unicodedata
import hashlib
from supabase import create_client

st.set_page_config(page_title="Buscador de Talentos", page_icon="🎯", layout="wide")

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

# ─── DESIGN SYSTEM ───────────────────────────────────────────────────────────

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --tap-bg: #0a0f1a;
    --tap-surface: #111827;
    --tap-surface-hover: #1a2332;
    --tap-border: #1e293b;
    --tap-border-hover: #334155;
    --tap-text: #f1f5f9;
    --tap-text-secondary: #94a3b8;
    --tap-text-muted: #64748b;
    --tap-accent: #6366f1;
    --tap-accent-soft: rgba(99, 102, 241, 0.12);
    --tap-green: #10b981;
    --tap-green-soft: rgba(16, 185, 129, 0.12);
    --tap-amber: #f59e0b;
    --tap-amber-soft: rgba(245, 158, 11, 0.12);
    --tap-rose: #f43f5e;
    --tap-rose-soft: rgba(244, 63, 94, 0.12);
    --tap-cyan: #06b6d4;
    --tap-cyan-soft: rgba(6, 182, 212, 0.12);
    --tap-purple: #a78bfa;
    --tap-purple-soft: rgba(167, 139, 250, 0.12);
    --tap-orange: #fb923c;
    --tap-orange-soft: rgba(251, 146, 60, 0.12);
    --tap-radius: 14px;
    --tap-radius-sm: 8px;
    --tap-radius-xs: 5px;
}

/* ── Global ── */
.main .block-container { max-width: 1200px; padding-top: 2rem; }
h1, h2, h3, h4, h5, h6, p, span, div, label, li {
    font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif !important;
}

/* ── Hero Header ── */
.tap-hero {
    background: linear-gradient(135deg, #111827 0%, #1e1b4b 50%, #111827 100%);
    border: 1px solid var(--tap-border);
    border-radius: var(--tap-radius);
    padding: 2.2rem 2.5rem;
    margin-bottom: 1.5rem;
    position: relative;
    overflow: hidden;
}
.tap-hero::before {
    content: '';
    position: absolute;
    top: -60%;
    right: -20%;
    width: 400px;
    height: 400px;
    background: radial-gradient(circle, rgba(99,102,241,0.08) 0%, transparent 70%);
    pointer-events: none;
}
.tap-hero h1 {
    font-size: 1.9rem;
    font-weight: 700;
    color: var(--tap-text);
    margin: 0 0 .3rem 0;
    letter-spacing: -0.02em;
}
.tap-hero p {
    color: var(--tap-text-muted);
    font-size: .95rem;
    margin: 0;
}

/* ── KPI Strip ── */
.tap-kpi-strip {
    display: flex;
    gap: 1rem;
    margin-bottom: 1.5rem;
}
.tap-kpi {
    flex: 1;
    background: var(--tap-surface);
    border: 1px solid var(--tap-border);
    border-radius: var(--tap-radius-sm);
    padding: 1rem 1.2rem;
    text-align: center;
    transition: border-color .2s;
}
.tap-kpi:hover { border-color: var(--tap-border-hover); }
.tap-kpi-label {
    font-size: .72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .08em;
    color: var(--tap-text-muted);
    margin-bottom: .25rem;
}
.tap-kpi-value {
    font-size: 1.65rem;
    font-weight: 700;
    color: var(--tap-text);
    font-family: 'JetBrains Mono', monospace !important;
}
.tap-kpi-value.accent { color: var(--tap-accent); }
.tap-kpi-value.green { color: var(--tap-green); }

/* ── Result count badge ── */
.tap-results-badge {
    display: inline-flex;
    align-items: center;
    gap: .5rem;
    background: var(--tap-accent-soft);
    color: var(--tap-accent);
    padding: .4rem 1rem;
    border-radius: 20px;
    font-size: .85rem;
    font-weight: 600;
    margin-bottom: 1rem;
}
.tap-results-badge .dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: var(--tap-accent);
    animation: pulse-dot 2s infinite;
}
@keyframes pulse-dot {
    0%, 100% { opacity: 1; }
    50% { opacity: .3; }
}

/* ── Lead Card ── */
.tap-card {
    background: var(--tap-surface);
    border: 1px solid var(--tap-border);
    border-radius: var(--tap-radius);
    padding: 1.3rem 1.5rem;
    margin-bottom: .75rem;
    transition: all .25s cubic-bezier(.4, 0, .2, 1);
    position: relative;
}
.tap-card:hover {
    border-color: var(--tap-accent);
    box-shadow: 0 0 0 1px var(--tap-accent), 0 8px 25px -5px rgba(99, 102, 241, 0.15);
    transform: translateY(-1px);
}
.tap-card-header {
    display: flex;
    align-items: center;
    gap: 1rem;
}

/* Avatar */
.tap-avatar {
    width: 48px;
    height: 48px;
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.15rem;
    font-weight: 700;
    color: #fff;
    flex-shrink: 0;
    letter-spacing: -0.02em;
    position: relative;
}
.tap-avatar.lideranca { background: linear-gradient(135deg, #6366f1, #8b5cf6); }
.tap-avatar.senior { background: linear-gradient(135deg, #06b6d4, #0ea5e9); }
.tap-avatar.pleno { background: linear-gradient(135deg, #10b981, #34d399); }
.tap-avatar.junior { background: linear-gradient(135deg, #f59e0b, #fbbf24); }
.tap-avatar.default { background: linear-gradient(135deg, #64748b, #94a3b8); }

.tap-card-info { flex: 1; min-width: 0; }
.tap-card-name {
    font-size: 1.05rem;
    font-weight: 600;
    color: var(--tap-text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    line-height: 1.3;
}
.tap-card-role {
    font-size: .85rem;
    color: var(--tap-text-secondary);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.tap-card-company {
    font-size: .8rem;
    color: var(--tap-text-muted);
    display: flex;
    align-items: center;
    gap: .35rem;
}
.tap-card-company::before {
    content: '●';
    font-size: .45rem;
    color: var(--tap-text-muted);
}

/* Tags */
.tap-tags { display: flex; flex-wrap: wrap; gap: 5px; margin-top: .7rem; }
.tap-tag {
    display: inline-flex;
    align-items: center;
    padding: 3px 10px;
    border-radius: var(--tap-radius-xs);
    font-size: .72rem;
    font-weight: 500;
    letter-spacing: .01em;
}
.tap-tag.senioridade { background: var(--tap-accent-soft); color: var(--tap-accent); }
.tap-tag.senior-tag { background: var(--tap-cyan-soft); color: var(--tap-cyan); }
.tap-tag.pleno-tag { background: var(--tap-green-soft); color: var(--tap-green); }
.tap-tag.junior-tag { background: var(--tap-amber-soft); color: var(--tap-amber); }
.tap-tag.expertise { background: var(--tap-purple-soft); color: var(--tap-purple); }
.tap-tag.segmento { background: var(--tap-orange-soft); color: var(--tap-orange); }
.tap-tag.location { background: var(--tap-green-soft); color: var(--tap-green); }
.tap-tag.mercado { background: var(--tap-cyan-soft); color: var(--tap-cyan); }

/* Expandable tech sheet */
.tap-detail-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: .5rem .8rem;
    margin-top: .8rem;
    padding-top: .8rem;
    border-top: 1px solid var(--tap-border);
}
.tap-detail-item {
    display: flex;
    flex-direction: column;
}
.tap-detail-label {
    font-size: .68rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .06em;
    color: var(--tap-text-muted);
}
.tap-detail-value {
    font-size: .82rem;
    color: var(--tap-text-secondary);
    word-break: break-word;
}
.tap-detail-value a {
    color: var(--tap-accent);
    text-decoration: none;
}
.tap-detail-value a:hover { text-decoration: underline; }

/* Contact strip inside card */
.tap-contact-strip {
    display: flex;
    gap: 1rem;
    margin-top: .6rem;
    padding-top: .6rem;
    border-top: 1px dashed var(--tap-border);
}
.tap-contact-item {
    font-size: .78rem;
    color: var(--tap-text-muted);
    display: flex;
    align-items: center;
    gap: .3rem;
}
.tap-contact-item a { color: var(--tap-accent); text-decoration: none; }
.tap-contact-item a:hover { text-decoration: underline; }

/* ── Empty state ── */
.tap-empty {
    text-align: center;
    padding: 5rem 2rem;
    color: var(--tap-text-muted);
}
.tap-empty h2 {
    font-size: 1.4rem;
    font-weight: 600;
    color: var(--tap-text-secondary);
    margin-bottom: .5rem;
}
.tap-empty p { font-size: .9rem; }
.tap-empty .icon {
    font-size: 3rem;
    margin-bottom: 1rem;
    opacity: .5;
}

/* ── Sidebar tweaks ── */
section[data-testid="stSidebar"] .stFileUploader label { font-weight: 500; }

/* ── Export bar ── */
.tap-export-bar {
    display: flex;
    align-items: center;
    gap: .8rem;
    margin-bottom: 1rem;
}
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ─── SUPABASE & DATA FUNCTIONS (unchanged pipeline) ─────────────────────────

@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])


def count_leads():
    try:
        sb = get_supabase()
        result = sb.table(TABLE).select("id", count="exact").limit(1).execute()
        return result.count or 0
    except Exception:
        return 0


def get_filter_options():
    sb = get_supabase()
    options = {}
    for col in ['senioridade_normalizada', 'expertise', 'segmento_empresa', 'segmento_mercado']:
        try:
            result = sb.rpc("get_distinct_values", {"col_name": col}).execute()
            if result.data:
                options[col] = sorted([r['val'] for r in result.data if r['val'] and r['val'] not in ('nan', '', 'None')])
            else:
                options[col] = []
        except Exception:
            try:
                result = sb.table(TABLE).select(col).limit(5000).execute()
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
    result = q.limit(limit).execute()
    return result.data or []


def insert_leads_batch(records, progress_callback=None):
    sb = get_supabase()
    total = len(records)
    inserted = 0
    errors = []
    for i in range(0, total, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            sb.table(TABLE).insert(batch).execute()
            inserted += len(batch)
            if progress_callback:
                pct = 75 + int((i / total) * 25)
                progress_callback(min(pct, 99), f"Enviando... {inserted:,}/{total:,}")
        except Exception as e:
            err_msg = str(e)
            if 'duplicate' in err_msg.lower() or '23505' in err_msg:
                for record in batch:
                    try:
                        sb.table(TABLE).insert(record).execute()
                        inserted += 1
                    except Exception:
                        pass
            else:
                errors.append(f"Lote {i//BATCH_SIZE + 1}: {err_msg[:500]}")
                if len(errors) == 1:
                    sample = {k: str(v)[:50] for k, v in batch[0].items() if v is not None}
                    errors.append(f"Amostra: {sample}")
                if len(errors) >= 6:
                    break
    if errors:
        st.session_state['pipeline_errors'] = errors
    return inserted


# ─── DATA CLEANING & NORMALIZATION (unchanged) ──────────────────────────────

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', str(s)) if unicodedata.category(c) != 'Mn').lower().strip()


def _norm_str(s):
    if pd.isna(s):
        return None
    s = str(s).strip().lower()
    return s if s not in ('', 'nan', 'none', 'n/a') else None


def _norm_linkedin(url):
    if pd.isna(url):
        return None
    url = str(url).strip().lower().split('?')[0].rstrip('/')
    if '/in/' in url:
        slug = url.split('/in/')[-1].rstrip('/')
        return slug if slug else None
    return None


def _safe_clean(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if s.lower() in ('nan', 'none', '', 'nat', 'n/a'):
        return None
    return s


def limpar_localizacao(df):
    df = df.copy()
    for col in ['Cidade', 'UF', 'Estado', 'Pais', 'location']:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].astype(str).replace('nan', '')
    uf_map = {'Federal District': 'Brasilia', 'Brasilia': 'Brasilia'}
    for old, new in uf_map.items():
        df.loc[df['UF'] == old, 'UF'] = new
    cidade_map = {'Federal District': 'Brasilia', 'Distrito Federal': 'Brasilia', 'Brasilia': 'Brasilia'}
    for old, new in cidade_map.items():
        df.loc[df['Cidade'] == old, 'Cidade'] = new
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

EXPERTISE_SEN_MAP = {
    'Dados e BI': 'Pleno', 'Engenharia de software': 'Pleno',
    'Agile': 'Senior/Especialista', 'Recrutamento e selecao': 'Pleno',
    'Operacoes e processos': 'Pleno', 'Financas e controladoria': 'Senior/Especialista',
    'Customer success': 'Pleno',
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
    jt_full = strip_accents(str(jt).lower()) if pd.notna(jt) else ''
    if re.search(r'(estagio|estagiario|estagiaria|trainee|aprendiz|intern\b|apprentice)', texto): return 'Junior/Estagio'
    if re.search(r'(\bjunior\b|\bjr\b)', texto): return 'Junior/Estagio'
    if re.search(r'\bpleno\b', texto): return 'Pleno'
    if re.search(r'(\bsenior\b|\bsr\b|\bespecialista\b|\blead\b|\bstaff\b|\bprincipal\b)', texto): return 'Senior/Especialista'
    if re.search(r'(diretor|diretora|ceo|cto|cfo|coo|cio|chro|ciso|cmo|head |founder|co-founder|gerente|manager|coordenador|supervisor|\bvp\b|vice.president|managing|presidente)', texto): return 'Lideranca/Executivo'
    if re.search(r'(\biii\b|\biv\b|\bn3\b|\bn4\b)', texto): return 'Senior/Especialista'
    if re.search(r'(\bii\b|\bn2\b)', texto): return 'Pleno'
    if re.search(r'\bn1\b', texto): return 'Junior/Estagio'
    if re.search(r'(estagio|estagiario|trainee|intern\b|\bjunior\b|\bjr\b)', jt_full): return 'Junior/Estagio'
    if re.search(r'\bpleno\b', jt_full): return 'Pleno'
    if re.search(r'(\bsenior\b|\bsr\b|\bespecialista\b|\blead\b|\bstaff\b|\bprincipal\b|\biii\b|\biv\b)', jt_full): return 'Senior/Especialista'
    if re.search(r'(diretor|diretora|ceo|cto|gerente|manager|coordenador|supervisor|\bvp\b|head |founder|presidente|executive\b)', jt_full): return 'Lideranca/Executivo'
    cat18_s = str(cat18).strip()
    if ' - ' in cat18_s:
        suffix = cat18_s.split(' - ', 1)[1].strip()
        mapped = CAT18_SEN_MAP.get(suffix)
        if mapped and mapped != 'Nao Identificado': return mapped
    prefix18 = cat18_s.split(' - ')[0].strip() if ' - ' in cat18_s else cat18_s
    if prefix18 in ('Executivo/Dono', 'Gestao/Lideranca'): return 'Lideranca/Executivo'
    return 'Nao Identificado'


def _build_lookups(dataframe):
    if 'Nivel_Senioridade' not in dataframe.columns:
        return {}, {}, {}, {}, {}, {}
    has_nivel = dataframe['Nivel_Senioridade'].notna() & (~dataframe['Nivel_Senioridade'].astype(str).isin(['', 'nan', 'NaN']))
    base = dataframe[has_nivel].copy()
    if base.empty:
        return {}, {}, {}, {}, {}, {}
    def _norm_nivel(n):
        n = strip_accents(str(n).lower())
        if any(x in n for x in ('estagio','trainee','junior','aprendiz')): return 'Junior/Estagio'
        if 'pleno' in n: return 'Pleno'
        if any(x in n for x in ('senior','especialista')): return 'Senior/Especialista'
        if any(x in n for x in ('lideranca','executivo','coordenacao','gerencia')): return 'Lideranca/Executivo'
        return None
    base['_sen'] = base['Nivel_Senioridade'].apply(_norm_nivel)
    base['_cn'] = base['Cargo'].fillna('').apply(lambda x: strip_accents(str(x).lower().strip()))
    base['_exp'] = base.get('Expertise', pd.Series(dtype=str)).fillna('').astype(str).str.strip()
    base = base[base['_sen'].notna()]
    _cols = ['Junior/Estagio','Pleno','Senior/Especialista','Lideranca/Executivo']
    def _make_lookup(grp_col, min_count=5, min_pct=0.40):
        if grp_col not in base.columns or base[grp_col].isna().all():
            return {}, {}
        piv = base.groupby([grp_col, '_sen']).size().unstack(fill_value=0)
        c = [x for x in _cols if x in piv.columns]
        if not c: return {}, {}
        piv['t'] = piv[c].sum(axis=1)
        piv = piv[piv['t'] >= min_count]
        if piv.empty: return {}, {}
        piv['maj'] = piv[c].idxmax(axis=1)
        piv['pct'] = piv[c].max(axis=1) / piv['t']
        high = piv[piv['pct'] >= 0.55]['maj'].to_dict()
        approx = piv[(piv['pct'] >= min_pct) & (piv['pct'] < 0.55)]['maj'].to_dict()
        return high, approx
    cargo_high, cargo_approx = _make_lookup('_cn', min_count=5)
    exp_high, exp_approx = _make_lookup('_exp', min_count=10)
    seg_high, seg_approx = {}, {}
    if 'segmento_mercado' in dataframe.columns:
        base['_seg'] = dataframe.loc[base.index, 'segmento_mercado'].fillna('')
        seg_high, seg_approx = _make_lookup('_seg', min_count=20)
    return cargo_high, cargo_approx, exp_high, exp_approx, seg_high, seg_approx


def _rescue(cargo, jt, cat18, segmento, expertise, lookups):
    cargo_high, cargo_approx, exp_high, exp_approx, seg_high, seg_approx = lookups
    jt_first = str(jt).split('|')[0] if pd.notna(jt) else ''
    texto = strip_accents((str(cargo) + ' ' + jt_first).lower())
    jt_full = strip_accents(str(jt).lower()) if pd.notna(jt) else ''
    combined = texto + ' ' + jt_full
    if re.search(r'(\biii\b|\biv\b|\bn3\b|\bn4\b)', combined): return 'Senior/Especialista', False
    if re.search(r'(\bii\b|\bn2\b)', combined): return 'Pleno', False
    if re.search(r'\bn1\b', combined): return 'Junior/Estagio', False
    if re.search(r'(estagio|estagiario|trainee|intern\b|\bjunior\b|\bjr\b)', jt_full): return 'Junior/Estagio', False
    if re.search(r'\bpleno\b', jt_full): return 'Pleno', False
    if re.search(r'(\bsenior\b|\bsr\b|\bespecialista\b|\blead\b|\bstaff\b|\bprincipal\b|\biii\b|\biv\b)', jt_full): return 'Senior/Especialista', False
    if re.search(r'(diretor|diretora|ceo|cto|gerente|manager|coordenador|supervisor|\bvp\b|head |founder|presidente|executive\b)', jt_full): return 'Lideranca/Executivo', False
    cat18_s = str(cat18).strip()
    if ' - ' in cat18_s:
        suffix = cat18_s.split(' - ', 1)[1].strip()
        mapped = CAT18_SEN_MAP.get(suffix)
        if mapped and mapped != 'Nao Identificado': return mapped, False
    prefix18 = cat18_s.split(' - ')[0].strip() if ' - ' in cat18_s else cat18_s
    if prefix18 in ('Executivo/Dono', 'Gestao/Lideranca'): return 'Lideranca/Executivo', False
    if str(segmento) == 'Executivo/Dono': return 'Lideranca/Executivo', False
    exp = str(expertise).strip()
    if exp in EXPERTISE_SEN_MAP: return EXPERTISE_SEN_MAP[exp], False
    cn = strip_accents(str(cargo).lower().strip())
    if cn in cargo_high: return cargo_high[cn], False
    if cn in cargo_approx: return cargo_approx[cn], True
    if exp in exp_approx and exp not in ('', 'nan'): return exp_approx[exp], True
    seg = str(segmento).strip()
    if seg in seg_approx and seg not in ('Sem Cargo Informado', 'Outros/Nao Identificado'):
        return seg_approx[seg], True
    return 'Nao Identificado', False


def normalizar_senioridade(df):
    for col in ['Nivel_Senioridade', 'Cargo', 'job_title', 'Categoria_Cargo', 'segmento_mercado', 'Expertise']:
        if col not in df.columns:
            df[col] = ''
    lookups = _build_lookups(df)
    df['senioridade_normalizada'] = df.apply(lambda r: _inferir_exact(r.get('Nivel_Senioridade', ''), r.get('Cargo', ''), r.get('job_title', ''), r.get('Categoria_Cargo', '')), axis=1)
    df['senioridade_aproximada'] = False
    mask_nao = df['senioridade_normalizada'] == 'Nao Identificado'
    if mask_nao.any():
        rescued = df[mask_nao].apply(lambda r: _rescue(r.get('Cargo', ''), r.get('job_title', ''), r.get('Categoria_Cargo', ''), r.get('segmento_mercado', ''), r.get('Expertise', ''), lookups), axis=1)
        df.loc[mask_nao, 'senioridade_normalizada'] = rescued.apply(lambda x: x[0])
        df.loc[mask_nao, 'senioridade_aproximada'] = rescued.apply(lambda x: x[1])
    return df


def deduplicar_local(df):
    total_antes = len(df)
    for col in ['salesNavigatorId', 'linkedinUrl', 'linkedinEmail']:
        if col not in df.columns:
            df[col] = None
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
    return df, total_antes, len(df)


def pipeline_e_salva(uploaded_file, progress_callback=None):
    DATAIKU_SCHEMA = [
        'Sheet Name','Origem','name','firstName','lastName',
        'Nivel_Senioridade','Expertise','Segmento_Empresa','DDI','DDD_Telefone',
        'linkedinEmail','company_name','segmento_mercado','Cargo','occupation',
        'job_title','linkedinUrl','Recrutador?','Publico_Alvo_Ads','gender',
        'state','Cidade','UF','Estado','Pais','location','premium','jobSeeker',
        'profileStatus','messageSent','messageReplied','emailSent','emailReplied',
        'salesNavigatorId','connectedAt','profilePictureUrl','importDate',
        'company_website','company_linkedinUrl','crmStatus','firstMessageAt',
        'lastLinkedinReplyDate','lastEmailReplyDate','lastLinkedinMessageSentDate',
        'lastEmailSentDate','connectionRequestDate',
        'senioridade_normalizada','senioridade_aproximada',
    ]
    if progress_callback: progress_callback(5, "Lendo arquivo...")
    name = uploaded_file.name.lower()
    if name.endswith('.csv'):
        df = pd.read_csv(uploaded_file, dtype=str)
        known_cols = {'name', 'linkedinEmail', 'Cargo', 'linkedinUrl', 'company_name', 'Cidade'}
        if not known_cols.intersection(set(df.columns)):
            if progress_callback: progress_callback(8, "CSV sem cabecalho detectado...")
            uploaded_file.seek(0)
            if len(df.columns) == len(DATAIKU_SCHEMA):
                df = pd.read_csv(uploaded_file, dtype=str, header=None, names=DATAIKU_SCHEMA)
            else:
                df = pd.read_csv(uploaded_file, dtype=str, header=None)
                for i, col_name in enumerate(DATAIKU_SCHEMA[:len(df.columns)]):
                    df = df.rename(columns={i: col_name})
    elif name.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(uploaded_file, dtype=str)
        known_cols = {'name', 'linkedinEmail', 'Cargo', 'linkedinUrl', 'company_name', 'Cidade'}
        if not known_cols.intersection(set(df.columns)):
            if len(df.columns) == len(DATAIKU_SCHEMA):
                uploaded_file.seek(0)
                df = pd.read_excel(uploaded_file, dtype=str, header=None, names=DATAIKU_SCHEMA)
    else:
        raise ValueError("Use CSV ou XLSX")
    total_raw = len(df)
    for col in ['Cidade', 'UF', 'Estado', 'Pais', 'location', 'Nivel_Senioridade', 'Cargo', 'job_title', 'Categoria_Cargo', 'segmento_mercado', 'Expertise', 'salesNavigatorId', 'linkedinUrl', 'linkedinEmail']:
        if col not in df.columns:
            df[col] = ''
    if progress_callback: progress_callback(20, "Limpando localizacao...")
    df = limpar_localizacao(df)
    if progress_callback: progress_callback(40, "Normalizando senioridade...")
    df = normalizar_senioridade(df)
    if progress_callback: progress_callback(60, "Deduplicando...")
    df, antes, depois = deduplicar_local(df)
    if progress_callback: progress_callback(70, f"Preparando {depois:,} leads...")
    df_out = pd.DataFrame()
    for orig, dest in COL_MAP.items():
        if orig in df.columns:
            df_out[dest] = df[orig].apply(_safe_clean)
        else:
            df_out[dest] = None
    slugs = []
    for idx, row in df.iterrows():
        slug = row.get('_linkedin_slug')
        if not slug or pd.isna(slug):
            email = _norm_str(row.get('linkedinEmail', ''))
            if email:
                slug = f"email:{email}"
            else:
                snid = _norm_str(row.get('salesNavigatorId', ''))
                if snid:
                    slug = f"snid:{snid}"
                else:
                    raw = f"{row.get('name','')}|{row.get('Cargo','')}|{row.get('company_name','')}"
                    slug = f"hash:{hashlib.md5(raw.encode()).hexdigest()[:16]}"
        slugs.append(slug)
    df_out['linkedin_slug'] = slugs
    if progress_callback: progress_callback(75, f"Enviando {depois:,} leads pro banco...")
    records = df_out.to_dict(orient='records')
    for r in records:
        for k, v in list(r.items()):
            if v is None:
                continue
            try:
                if pd.isna(v):
                    r[k] = None
                    continue
            except (TypeError, ValueError):
                pass
            if str(v).strip().lower() in ('nan', 'none', '', 'nat', 'n/a'):
                r[k] = None
        sa = r.get('senioridade_aproximada')
        if sa is not None:
            r['senioridade_aproximada'] = str(sa).lower() in ('true', '1', 'yes')
        else:
            r['senioridade_aproximada'] = False
    inserted = insert_leads_batch(records, progress_callback)
    if progress_callback: progress_callback(100, f"Pronto! {inserted:,} leads salvos!")
    return {'arquivo': uploaded_file.name, 'linhas_raw': total_raw, 'antes_dedup': antes, 'depois_dedup': depois, 'inseridos': inserted}


# ─── CARD RENDERING HELPERS ─────────────────────────────────────────────────

def _get_initials(name):
    """Generate avatar initials from name."""
    if not name or name == '-':
        return '?'
    parts = name.strip().split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()
    return parts[0][0].upper()


def _get_avatar_class(sen):
    """Map seniority to avatar gradient class."""
    s = (sen or '').lower()
    if 'lideranca' in s or 'executivo' in s:
        return 'lideranca'
    if 'senior' in s or 'especialista' in s:
        return 'senior'
    if 'pleno' in s:
        return 'pleno'
    if 'junior' in s or 'estagio' in s:
        return 'junior'
    return 'default'


def _get_senioridade_tag_class(sen):
    """Map seniority to tag color class."""
    s = (sen or '').lower()
    if 'lideranca' in s or 'executivo' in s:
        return 'senioridade'
    if 'senior' in s or 'especialista' in s:
        return 'senior-tag'
    if 'pleno' in s:
        return 'pleno-tag'
    if 'junior' in s or 'estagio' in s:
        return 'junior-tag'
    return 'senioridade'


def _esc(text):
    """Escape HTML entities in text for safe rendering."""
    if not text:
        return ''
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def render_lead_card(r, idx):
    """Render a single lead card with avatar, tags, and expandable details."""
    nome = r.get('name') or '-'
    cargo = r.get('cargo') or r.get('occupation') or '-'
    company = r.get('company_name') or '-'
    sen = r.get('senioridade_normalizada') or ''
    expertise = r.get('expertise') or ''
    segmento = r.get('segmento_empresa') or ''
    mercado = r.get('segmento_mercado') or ''
    cidade = r.get('cidade') or ''
    uf = r.get('uf') or r.get('estado') or ''
    email = r.get('linkedin_email') or ''
    tel = r.get('ddd_telefone') or ''
    li_url = r.get('linkedin_url') or ''
    job_title = r.get('job_title') or ''
    recrutador = r.get('recrutador') or ''

    initials = _get_initials(nome)
    avatar_cls = _get_avatar_class(sen)
    sen_tag_cls = _get_senioridade_tag_class(sen)

    # Build tags HTML
    tags = ''
    if sen and sen != 'Nao Identificado':
        tags += f'<span class="tap-tag {sen_tag_cls}">{_esc(sen)}</span>'
    if expertise:
        tags += f'<span class="tap-tag expertise">{_esc(expertise)}</span>'
    if segmento:
        tags += f'<span class="tap-tag segmento">{_esc(segmento)}</span>'
    if mercado:
        tags += f'<span class="tap-tag mercado">{_esc(mercado)}</span>'
    loc_parts = [p for p in [cidade, uf] if p]
    if loc_parts:
        tags += f'<span class="tap-tag location">📍 {_esc(" / ".join(loc_parts))}</span>'
    if recrutador and str(recrutador).lower() in ('sim', 'yes', 'true', '1'):
        tags += '<span class="tap-tag" style="background:rgba(244,63,94,0.12);color:#f43f5e;">Recrutador</span>'

    # Contact strip
    contacts = ''
    if email and '@' in email:
        contacts += f'<div class="tap-contact-item">✉️ {_esc(email)}</div>'
    if tel:
        contacts += f'<div class="tap-contact-item">📞 {_esc(tel)}</div>'
    if li_url and 'linkedin' in li_url.lower():
        contacts += f'<div class="tap-contact-item"><a href="{_esc(li_url)}" target="_blank">🔗 LinkedIn</a></div>'

    contact_html = f'<div class="tap-contact-strip">{contacts}</div>' if contacts else ''

    # Card HTML (no expander - we'll use Streamlit expander for the tech sheet)
    card_html = f'''<div class="tap-card">
        <div class="tap-card-header">
            <div class="tap-avatar {avatar_cls}">{initials}</div>
            <div class="tap-card-info">
                <div class="tap-card-name">{_esc(nome)}</div>
                <div class="tap-card-role">{_esc(cargo)}</div>
                <div class="tap-card-company">{_esc(company)}</div>
            </div>
        </div>
        <div class="tap-tags">{tags}</div>
        {contact_html}
    </div>'''

    st.markdown(card_html, unsafe_allow_html=True)

    # Expandable tech sheet via Streamlit
    with st.expander(f"📋 Ficha técnica — {nome}", expanded=False):
        dc1, dc2 = st.columns(2)
        with dc1:
            st.markdown(f"**Nome:** {_esc(nome)}")
            st.markdown(f"**Cargo:** {_esc(cargo)}")
            if job_title and job_title != cargo:
                st.markdown(f"**Job Title:** {_esc(job_title)}")
            st.markdown(f"**Empresa:** {_esc(company)}")
            st.markdown(f"**Senioridade:** {_esc(sen) if sen else '—'}")
            if r.get('senioridade_aproximada') and str(r['senioridade_aproximada']).lower() in ('true', '1'):
                st.caption("⚠️ Senioridade aproximada (inferida)")
        with dc2:
            st.markdown(f"**Expertise:** {_esc(expertise) if expertise else '—'}")
            st.markdown(f"**Segmento:** {_esc(segmento) if segmento else '—'}")
            st.markdown(f"**Mercado:** {_esc(mercado) if mercado else '—'}")
            loc_str = ' / '.join(loc_parts) if loc_parts else '—'
            st.markdown(f"**Localização:** {loc_str}")
            if email and '@' in email:
                st.markdown(f"**Email:** {email}")
            if tel:
                st.markdown(f"**Telefone:** {tel}")
            if li_url and 'linkedin' in li_url.lower():
                st.markdown(f"**LinkedIn:** [{li_url[:50]}...]({li_url})" if len(li_url) > 50 else f"**LinkedIn:** [{li_url}]({li_url})")


# ─── SIDEBAR: UPLOAD PIPELINE ───────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 📂 Importar Planilha")
    total = count_leads()
    if total > 0:
        st.success(f"Banco ativo: **{total:,}** leads")
    else:
        st.info("Banco vazio — suba sua primeira planilha!")

    st.markdown("---")
    uploaded = st.file_uploader("Arraste CSV ou XLSX aqui", type=['csv', 'xlsx', 'xls'], label_visibility="collapsed")

    if uploaded and st.button("🚀 Processar e Salvar", type="primary", use_container_width=True):
        progress_bar = st.progress(0)
        spinner_placeholder = st.empty()

        def update_progress(pct, msg):
            progress_bar.progress(pct / 100)
            spinner_placeholder.markdown(
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<div style="width:18px;height:18px;border:2.5px solid #334155;border-top:2.5px solid #6366f1;'
                f'border-radius:50%;animation:spin 1s linear infinite;"></div>'
                f'<span style="font-size:.82rem;color:#94a3b8;">{msg}</span></div>'
                f'<style>@keyframes spin{{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}</style>',
                unsafe_allow_html=True
            )

        try:
            result = pipeline_e_salva(uploaded, update_progress)
            spinner_placeholder.empty()
            st.session_state['pipeline_success'] = (
                f"**{result['arquivo']}** — "
                f"Linhas: {result['linhas_raw']:,} · "
                f"Dedup: {result['depois_dedup']:,} · "
                f"Salvos: {result['inseridos']:,}"
            )
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            import traceback
            spinner_placeholder.empty()
            st.session_state['pipeline_errors'] = [f"Erro: {e}", traceback.format_exc()]
            st.rerun()

    st.markdown("---")
    st.caption("Cada planilha passa pelo pipeline completo (limpeza → normalização → deduplicação) e é adicionada ao banco.")

    if count_leads() > 0:
        st.markdown("---")
        with st.expander("⚙️ Manutenção"):
            if st.button("🗑️ LIMPAR BANCO", use_container_width=True):
                sb = get_supabase()
                with st.spinner("Limpando..."):
                    try:
                        while True:
                            batch = sb.table(TABLE).select("id").limit(500).execute()
                            ids = [x["id"] for x in batch.data]
                            if not ids:
                                break
                            sb.table(TABLE).delete().in_("id", ids).execute()
                        st.success("Banco limpo!")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")


# ─── MAIN CONTENT ───────────────────────────────────────────────────────────

# Hero
st.markdown(
    '<div class="tap-hero">'
    '<h1>🎯 Buscador de Talentos</h1>'
    '<p>Pesquise, filtre e exporte leads qualificados do seu banco de talentos</p>'
    '</div>',
    unsafe_allow_html=True
)

# Pipeline feedback
if st.session_state.get('pipeline_errors'):
    st.error("Erros durante o upload:")
    for err in st.session_state['pipeline_errors'][:5]:
        st.code(err, language="text")
    if st.button("Fechar erros"):
        del st.session_state['pipeline_errors']
        st.rerun()

if st.session_state.get('pipeline_success'):
    st.success(st.session_state['pipeline_success'])
    if st.button("Fechar"):
        del st.session_state['pipeline_success']
        st.rerun()

# Empty state
total = count_leads()
if total == 0:
    st.markdown(
        '<div class="tap-empty">'
        '<div class="icon">📭</div>'
        '<h2>Nenhum lead no banco</h2>'
        '<p>Use a barra lateral para subir sua primeira planilha.</p>'
        '</div>',
        unsafe_allow_html=True
    )
    st.stop()

# KPI strip
st.markdown(
    f'<div class="tap-kpi-strip">'
    f'<div class="tap-kpi"><div class="tap-kpi-label">Total de Leads</div>'
    f'<div class="tap-kpi-value accent">{total:,}</div></div>'
    f'<div class="tap-kpi"><div class="tap-kpi-label">Banco</div>'
    f'<div class="tap-kpi-value green">Supabase</div></div>'
    f'<div class="tap-kpi"><div class="tap-kpi-label">Status</div>'
    f'<div class="tap-kpi-value green">● Online</div></div>'
    f'</div>',
    unsafe_allow_html=True
)

# Filters
@st.cache_data(ttl=300)
def cached_filter_options():
    return get_filter_options()

options = cached_filter_options()

with st.expander("🔎 Filtros avançados", expanded=True):
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        fil_senioridade = st.multiselect("Senioridade", options.get('senioridade_normalizada', []))
    with fc2:
        fil_expertise = st.multiselect("Expertise", options.get('expertise', []))
    with fc3:
        fil_segmento = st.multiselect("Segmento empresa", options.get('segmento_empresa', []))
    fc4, fc5 = st.columns(2)
    with fc4:
        fil_mercado = st.multiselect("Área de mercado", options.get('segmento_mercado', []))
    with fc5:
        ESTADOS_BR = sorted([
            'AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT',
            'PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO'
        ])
        fil_estados = st.multiselect("Estado (UF)", ESTADOS_BR)

# Search bar + result limit slider
col_search, col_slider = st.columns([3, 1])
with col_search:
    consulta = st.text_input(
        "🔍 Busca livre",
        placeholder='Ex: head de produto SP  ·  engenheiro senior  ·  data analyst',
    )
with col_slider:
    LIMIT_OPTIONS = {50: "50", 100: "100", 500: "500", 10000: "Todos"}
    limit_val = st.select_slider(
        "Resultados",
        options=list(LIMIT_OPTIONS.keys()),
        value=50,
        format_func=lambda x: LIMIT_OPTIONS[x],
    )

filters = {}
if fil_senioridade: filters['senioridade_normalizada'] = fil_senioridade
if fil_expertise: filters['expertise'] = fil_expertise
if fil_segmento: filters['segmento_empresa'] = fil_segmento
if fil_mercado: filters['segmento_mercado'] = fil_mercado
if fil_estados: filters['estados'] = fil_estados

if not (bool(filters) or bool(consulta.strip())):
    st.markdown(
        '<div class="tap-empty" style="padding:3rem;">'
        '<div class="icon">🔍</div>'
        '<h2>Pronto para buscar</h2>'
        '<p>Use os filtros ou a busca livre para encontrar leads qualificados.</p>'
        '</div>',
        unsafe_allow_html=True
    )
    st.stop()

# Execute search
with st.spinner("Buscando talentos..."):
    results = search_leads(query=consulta, filters=filters, limit=limit_val)

# Results header
n_results = len(results)
limit_label = LIMIT_OPTIONS[limit_val]
st.markdown(
    f'<div class="tap-results-badge">'
    f'<span class="dot"></span>'
    f'{n_results} resultado{"s" if n_results != 1 else ""} '
    f'(limite: {limit_label})'
    f'</div>',
    unsafe_allow_html=True
)

# Export buttons
if results:
    df_export = pd.DataFrame(results)
    cols_export = [c for c in COLUNAS_BANCO if c in df_export.columns]
    csv_data = df_export[cols_export].to_csv(index=False).encode('utf-8-sig')

    ecol1, ecol2, _ = st.columns([1, 1, 4])
    with ecol1:
        st.download_button("⬇️ Exportar CSV", csv_data, "resultados.csv", "text/csv")
    with ecol2:
        emails = [
            r.get('linkedin_email') for r in results
            if r.get('linkedin_email') and '@' in str(r.get('linkedin_email', ''))
        ]
        if emails:
            st.download_button("📧 Exportar emails", '\n'.join(emails), "emails.txt", "text/plain")

# Render cards
for idx, r in enumerate(results):
    render_lead_card(r, idx)
