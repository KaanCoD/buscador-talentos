"""
Buscador de Talentos - Streamlit + Supabase
Pipeline: Upload CSV/XLSX - Limpeza - Normalizacao - Deduplicacao - Supabase - Busca
"""
import streamlit as st
import pandas as pd
import re
import unicodedata
import hashlib
from supabase import create_client

st.set_page_config(page_title="Buscador de Talentos", page_icon="🔍", layout="wide")

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

# ── DESIGN SYSTEM (LIGHT / WHITE) ───────────────────────────────────────────

AVATAR_COLORS = [
    '#3b82f6', '#8b5cf6', '#06b6d4', '#10b981', '#f59e0b',
    '#ef4444', '#ec4899', '#6366f1', '#14b8a6', '#f97316',
    '#0ea5e9', '#a855f7', '#84cc16', '#e11d48', '#7c3aed',
]

SEN_TAG_COLORS = {
    'Lideranca/Executivo': ('#6366f1', '#eef2ff'),
    'Senior/Especialista': ('#0891b2', '#ecfeff'),
    'Pleno':              ('#059669', '#ecfdf5'),
    'Junior/Estagio':     ('#d97706', '#fffbeb'),
    'Nao Identificado':   ('#6b7280', '#f3f4f6'),
}

TAG_PALETTE = [
    ('#7c3aed', '#f5f3ff'),
    ('#0891b2', '#ecfeff'),
    ('#c2410c', '#fff7ed'),
    ('#be185d', '#fdf2f8'),
    ('#4338ca', '#eef2ff'),
    ('#0f766e', '#f0fdfa'),
]

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700&display=swap');

.main .block-container { max-width: 1280px; padding-top: 1.5rem; }
h1,h2,h3,h4,h5,h6,p,span,div,label,li {
    font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif !important;
}

.tap-header { display:flex; align-items:center; gap:.75rem; margin-bottom:.2rem; }
.tap-header-icon {
    width:34px; height:34px; background:#6366f1; border-radius:9px;
    display:flex; align-items:center; justify-content:center;
    color:#fff; font-size:.95rem; font-weight:700;
}
.tap-header h1 { font-size:1.4rem; font-weight:700; color:#111827; margin:0; letter-spacing:-.01em; }
.tap-header-sub { font-size:.82rem; color:#6b7280; margin:0 0 1rem 0; }

.tap-kpis { display:flex; gap:.6rem; margin-bottom:1rem; flex-wrap:wrap; }
.tap-kpi {
    background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px;
    padding:.5rem 1rem; display:flex; align-items:center; gap:.5rem;
}
.tap-kpi-dot { width:8px; height:8px; border-radius:50%; }
.tap-kpi-dot.blue { background:#6366f1; }
.tap-kpi-dot.green { background:#10b981; }
.tap-kpi-label { font-size:.72rem; color:#6b7280; }
.tap-kpi-val { font-size:.88rem; font-weight:700; color:#111827; }

.tap-rbadge {
    display:inline-flex; align-items:center; gap:.4rem;
    background:#f0fdf4; color:#166534; border:1px solid #bbf7d0;
    padding:.28rem .8rem; border-radius:6px;
    font-size:.8rem; font-weight:600; margin-bottom:.7rem;
}

.tap-grid {
    display:grid;
    grid-template-columns: repeat(auto-fill, minmax(330px, 1fr));
    gap:.7rem; margin-top:.5rem;
}
.tap-card {
    background:#fff; border:1px solid #e5e7eb; border-radius:11px;
    padding:.95rem 1.1rem; transition: box-shadow .2s, border-color .2s;
}
.tap-card:hover { border-color:#c7d2fe; box-shadow:0 4px 14px -2px rgba(99,102,241,.09); }

.tap-ch { display:flex; align-items:flex-start; gap:.7rem; }
.tap-av {
    width:40px; height:40px; border-radius:50%;
    display:flex; align-items:center; justify-content:center;
    font-size:.82rem; font-weight:700; color:#fff; flex-shrink:0;
}
.tap-ci { flex:1; min-width:0; }
.tap-cn {
    font-size:.92rem; font-weight:600; color:#111827;
    white-space:nowrap; overflow:hidden; text-overflow:ellipsis; line-height:1.3;
}
.tap-cr {
    font-size:.78rem; color:#4b5563;
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical;
    overflow:hidden; line-height:1.35; margin-top:1px;
}
.tap-cc {
    font-size:.72rem; color:#9ca3af; margin-top:2px;
    white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}

.tap-tags { display:flex; flex-wrap:wrap; gap:4px; margin-top:.5rem; }
.tap-tag {
    display:inline-block; padding:2px 8px; border-radius:4px;
    font-size:.67rem; font-weight:500; line-height:1.5; white-space:nowrap;
}
.tap-tag-rec { background:#fef2f2; color:#dc2626; }

.tap-contact {
    display:flex; flex-wrap:wrap; gap:.55rem;
    margin-top:.45rem; padding-top:.45rem; border-top:1px solid #f3f4f6;
}
.tap-contact a, .tap-contact span { font-size:.72rem; color:#6b7280; text-decoration:none; }
.tap-contact a:hover { color:#6366f1; text-decoration:underline; }

.tap-detail {
    background:#f9fafb; border:1px solid #e5e7eb; border-radius:8px;
    padding:.8rem 1rem; margin-top:.3rem;
}
.tap-detail-grid { display:grid; grid-template-columns:1fr 1fr; gap:.35rem .8rem; }
.tap-dl { font-size:.67rem; font-weight:600; text-transform:uppercase; letter-spacing:.04em; color:#9ca3af; }
.tap-dv { font-size:.78rem; color:#374151; word-break:break-word; }
.tap-dv a { color:#6366f1; text-decoration:none; }
.tap-dv a:hover { text-decoration:underline; }

.tap-empty { text-align:center; padding:4rem 2rem; color:#9ca3af; }
.tap-empty h2 { font-size:1.15rem; font-weight:600; color:#6b7280; margin-bottom:.3rem; }
.tap-empty p { font-size:.85rem; }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ── SUPABASE ─────────────────────────────────────────────────────────────────

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


# ── CLEANING & NORMALIZATION ─────────────────────────────────────────────────

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', str(s)) if unicodedata.category(c) != 'Mn').lower().strip()

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
    if 'Nivel_Senioridade' not in dataframe.columns: return {}, {}, {}, {}, {}, {}
    has_nivel = dataframe['Nivel_Senioridade'].notna() & (~dataframe['Nivel_Senioridade'].astype(str).isin(['', 'nan', 'NaN']))
    base = dataframe[has_nivel].copy()
    if base.empty: return {}, {}, {}, {}, {}, {}
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
        if grp_col not in base.columns or base[grp_col].isna().all(): return {}, {}
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
        if col not in df.columns: df[col] = ''
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
        if col not in df.columns: df[col] = ''
    if progress_callback: progress_callback(20, "Limpando localizacao...")
    df = limpar_localizacao(df)
    if progress_callback: progress_callback(40, "Normalizando senioridade...")
    df = normalizar_senioridade(df)
    if progress_callback: progress_callback(60, "Deduplicando...")
    df, antes, depois = deduplicar_local(df)
    if progress_callback: progress_callback(70, f"Preparando {depois:,} leads...")
    df_out = pd.DataFrame()
    for orig, dest in COL_MAP.items():
        if orig in df.columns: df_out[dest] = df[orig].apply(_safe_clean)
        else: df_out[dest] = None
    slugs = []
    for idx, row in df.iterrows():
        slug = row.get('_linkedin_slug')
        if not slug or pd.isna(slug):
            email = _norm_str(row.get('linkedinEmail', ''))
            if email: slug = f"email:{email}"
            else:
                snid = _norm_str(row.get('salesNavigatorId', ''))
                if snid: slug = f"snid:{snid}"
                else:
                    raw = f"{row.get('name','')}|{row.get('Cargo','')}|{row.get('company_name','')}"
                    slug = f"hash:{hashlib.md5(raw.encode()).hexdigest()[:16]}"
        slugs.append(slug)
    df_out['linkedin_slug'] = slugs
    if progress_callback: progress_callback(75, f"Enviando {depois:,} leads pro banco...")
    records = df_out.to_dict(orient='records')
    for r in records:
        for k, v in list(r.items()):
            if v is None: continue
            try:
                if pd.isna(v): r[k] = None; continue
            except (TypeError, ValueError): pass
            if str(v).strip().lower() in ('nan', 'none', '', 'nat', 'n/a'): r[k] = None
        sa = r.get('senioridade_aproximada')
        if sa is not None: r['senioridade_aproximada'] = str(sa).lower() in ('true', '1', 'yes')
        else: r['senioridade_aproximada'] = False
    inserted = insert_leads_batch(records, progress_callback)
    if progress_callback: progress_callback(100, f"Pronto! {inserted:,} leads salvos!")
    return {'arquivo': uploaded_file.name, 'linhas_raw': total_raw, 'antes_dedup': antes, 'depois_dedup': depois, 'inseridos': inserted}


# ── CARD RENDERING ───────────────────────────────────────────────────────────

def _esc(text):
    if not text: return ''
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

def _initials(name):
    if not name or name == '-': return '?'
    parts = name.strip().split()
    if len(parts) >= 2: return (parts[0][0] + parts[-1][0]).upper()
    return parts[0][0].upper()

def _avatar_color(name):
    h = sum(ord(c) for c in (name or '?'))
    return AVATAR_COLORS[h % len(AVATAR_COLORS)]

def _sen_tag(sen):
    fg, bg = SEN_TAG_COLORS.get(sen, SEN_TAG_COLORS['Nao Identificado'])
    return f'<span class="tap-tag" style="background:{bg};color:{fg};">{_esc(sen)}</span>'

def _extra_tag(text, idx=0):
    fg, bg = TAG_PALETTE[idx % len(TAG_PALETTE)]
    return f'<span class="tap-tag" style="background:{bg};color:{fg};">{_esc(text)}</span>'

def _loc_tag(text):
    return f'<span class="tap-tag" style="background:#f0fdf4;color:#15803d;">{_esc(text)}</span>'

def render_cards_html(results):
    cards = []
    for r in results:
        nome = r.get('name') or '-'
        cargo = r.get('cargo') or r.get('occupation') or '-'
        company = r.get('company_name') or ''
        sen = r.get('senioridade_normalizada') or ''
        expertise = r.get('expertise') or ''
        segmento = r.get('segmento_empresa') or ''
        mercado = r.get('segmento_mercado') or ''
        cidade = r.get('cidade') or ''
        uf = r.get('uf') or r.get('estado') or ''
        email = r.get('linkedin_email') or ''
        tel = r.get('ddd_telefone') or ''
        li_url = r.get('linkedin_url') or ''
        recrutador = r.get('recrutador') or ''

        ini = _initials(nome)
        acolor = _avatar_color(nome)

        tags = ''
        tag_idx = 0
        if sen and sen != 'Nao Identificado':
            tags += _sen_tag(sen)
        if expertise:
            tags += _extra_tag(expertise, tag_idx); tag_idx += 1
        if segmento:
            tags += _extra_tag(segmento, tag_idx); tag_idx += 1
        if mercado:
            tags += _extra_tag(mercado, tag_idx); tag_idx += 1
        loc_parts = [p for p in [cidade, uf] if p]
        if loc_parts:
            tags += _loc_tag(' / '.join(loc_parts))
        if recrutador and str(recrutador).lower() in ('sim', 'yes', 'true', '1'):
            tags += '<span class="tap-tag tap-tag-rec">Recrutador</span>'

        contacts = ''
        if email and '@' in email:
            contacts += f'<span>{_esc(email)}</span>'
        if tel:
            contacts += f'<span>{_esc(tel)}</span>'
        if li_url and 'linkedin' in li_url.lower():
            contacts += f'<a href="{_esc(li_url)}" target="_blank">Ver LinkedIn</a>'
        contact_html = f'<div class="tap-contact">{contacts}</div>' if contacts else ''
        company_html = f'<div class="tap-cc">{_esc(company)}</div>' if company else ''

        card = f'''<div class="tap-card">
  <div class="tap-ch">
    <div class="tap-av" style="background:{acolor};">{ini}</div>
    <div class="tap-ci">
      <div class="tap-cn">{_esc(nome)}</div>
      <div class="tap-cr">{_esc(cargo)}</div>
      {company_html}
    </div>
  </div>
  <div class="tap-tags">{tags}</div>
  {contact_html}
</div>'''
        cards.append(card)
    return '<div class="tap-grid">' + '\n'.join(cards) + '</div>'


# ── SIDEBAR ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Importar Planilha")
    total = count_leads()
    if total > 0:
        st.success(f"Banco ativo: **{total:,}** leads")
    else:
        st.info("Banco vazio -- suba sua primeira planilha!")
    st.markdown("---")
    uploaded = st.file_uploader("Arraste CSV ou XLSX aqui", type=['csv', 'xlsx', 'xls'], label_visibility="collapsed")
    if uploaded and st.button("Processar e Salvar", type="primary", use_container_width=True):
        progress_bar = st.progress(0)
        spinner_placeholder = st.empty()
        def update_progress(pct, msg):
            progress_bar.progress(pct / 100)
            spinner_placeholder.markdown(
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<div style="width:18px;height:18px;border:2.5px solid #e2e8f0;border-top:2.5px solid #6366f1;'
                f'border-radius:50%;animation:spin 1s linear infinite;"></div>'
                f'<span style="font-size:.82rem;color:#6b7280;">{msg}</span></div>'
                f'<style>@keyframes spin{{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}</style>',
                unsafe_allow_html=True)
        try:
            result = pipeline_e_salva(uploaded, update_progress)
            spinner_placeholder.empty()
            st.session_state['pipeline_success'] = (
                f"**{result['arquivo']}** -- "
                f"Linhas: {result['linhas_raw']:,} | "
                f"Dedup: {result['depois_dedup']:,} | "
                f"Salvos: {result['inseridos']:,}")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            import traceback
            spinner_placeholder.empty()
            st.session_state['pipeline_errors'] = [f"Erro: {e}", traceback.format_exc()]
            st.rerun()
    st.markdown("---")
    st.caption("Cada planilha passa pelo pipeline completo (limpeza > normalizacao > deduplicacao) e e adicionada ao banco.")
    if count_leads() > 0:
        st.markdown("---")
        with st.expander("Manutencao"):
            if st.button("LIMPAR BANCO", use_container_width=True):
                sb = get_supabase()
                with st.spinner("Limpando..."):
                    try:
                        while True:
                            batch = sb.table(TABLE).select("id").limit(500).execute()
                            ids = [x["id"] for x in batch.data]
                            if not ids: break
                            sb.table(TABLE).delete().in_("id", ids).execute()
                        st.success("Banco limpo!")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")


# ── MAIN ─────────────────────────────────────────────────────────────────────

st.markdown(
    '<div class="tap-header">'
    '<div class="tap-header-icon">BT</div>'
    '<h1>Buscador de Talentos</h1>'
    '</div>'
    '<p class="tap-header-sub">Combine filtros e texto livre -- clique num card para ver detalhes, ou selecione varios</p>',
    unsafe_allow_html=True)

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

total = count_leads()
if total == 0:
    st.markdown(
        '<div class="tap-empty"><h2>Nenhum lead no banco</h2>'
        '<p>Use a barra lateral para subir sua primeira planilha.</p></div>',
        unsafe_allow_html=True)
    st.stop()

st.markdown(
    f'<div class="tap-kpis">'
    f'<div class="tap-kpi"><div class="tap-kpi-dot blue"></div>'
    f'<div><div class="tap-kpi-label">Total de leads</div><div class="tap-kpi-val">{total:,}</div></div></div>'
    f'<div class="tap-kpi"><div class="tap-kpi-dot green"></div>'
    f'<div><div class="tap-kpi-label">Status</div><div class="tap-kpi-val">Online</div></div></div>'
    f'</div>',
    unsafe_allow_html=True)

consulta = st.text_input("Busca livre", placeholder='Ex: head de produto SP, engenheiro senior, data analyst')

@st.cache_data(ttl=300)
def cached_filter_options():
    return get_filter_options()

options = cached_filter_options()

with st.expander("Filtros", expanded=False):
    fc1, fc2, fc3 = st.columns(3)
    with fc1: fil_senioridade = st.multiselect("Senioridade", options.get('senioridade_normalizada', []))
    with fc2: fil_expertise = st.multiselect("Expertise", options.get('expertise', []))
    with fc3: fil_segmento = st.multiselect("Segmento empresa", options.get('segmento_empresa', []))
    fc4, fc5 = st.columns(2)
    with fc4: fil_mercado = st.multiselect("Area de mercado", options.get('segmento_mercado', []))
    with fc5:
        ESTADOS_BR = sorted(['AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO'])
        fil_estados = st.multiselect("Estado (UF)", ESTADOS_BR)

LIMIT_OPTIONS = {50: "50", 100: "100", 500: "500", 10000: "Todos"}
limit_val = st.select_slider("Quantidade de resultados", options=list(LIMIT_OPTIONS.keys()), value=50, format_func=lambda x: LIMIT_OPTIONS[x])

filters = {}
if fil_senioridade: filters['senioridade_normalizada'] = fil_senioridade
if fil_expertise: filters['expertise'] = fil_expertise
if fil_segmento: filters['segmento_empresa'] = fil_segmento
if fil_mercado: filters['segmento_mercado'] = fil_mercado
if fil_estados: filters['estados'] = fil_estados

if not (bool(filters) or bool(consulta.strip())):
    st.markdown(
        '<div class="tap-empty"><h2>Pronto para buscar</h2>'
        '<p>Use os filtros ou a busca livre acima para encontrar perfis.</p></div>',
        unsafe_allow_html=True)
    st.stop()

with st.spinner("Buscando..."):
    results = search_leads(query=consulta, filters=filters, limit=limit_val)

n_results = len(results)
st.markdown(f'<div class="tap-rbadge">{n_results} perfil(s) encontrado(s)</div>', unsafe_allow_html=True)

if results:
    df_export = pd.DataFrame(results)
    cols_export = [c for c in COLUNAS_BANCO if c in df_export.columns]
    csv_data = df_export[cols_export].to_csv(index=False).encode('utf-8-sig')
    ecol1, ecol2, _ = st.columns([1, 1, 4])
    with ecol1:
        st.download_button("Exportar CSV", csv_data, "resultados.csv", "text/csv")
    with ecol2:
        emails = [r.get('linkedin_email') for r in results if r.get('linkedin_email') and '@' in str(r.get('linkedin_email', ''))]
        if emails:
            st.download_button("Exportar emails", '\n'.join(emails), "emails.txt", "text/plain")

if results:
    st.markdown(render_cards_html(results), unsafe_allow_html=True)

    # Detail panel via selectbox (no broken emoji expanders)
    st.markdown("---")
    names_list = [f"{i+1}. {r.get('name', '-')}" for i, r in enumerate(results)]
    selected = st.selectbox("Abrir ficha tecnica de:", ["-- Selecione --"] + names_list)
    if selected != "-- Selecione --":
        idx = names_list.index(selected)
        r = results[idx]
        nome = r.get('name') or '-'
        cargo = r.get('cargo') or r.get('occupation') or '-'
        company = r.get('company_name') or '-'
        job_title = r.get('job_title') or ''
        sen = r.get('senioridade_normalizada') or '-'
        expertise = r.get('expertise') or '-'
        segmento = r.get('segmento_empresa') or '-'
        mercado = r.get('segmento_mercado') or '-'
        cidade = r.get('cidade') or ''
        uf = r.get('uf') or r.get('estado') or ''
        email = r.get('linkedin_email') or '-'
        tel = r.get('ddd_telefone') or '-'
        li_url = r.get('linkedin_url') or ''
        recrutador = r.get('recrutador') or '-'
        pais = r.get('pais') or ''
        publico = r.get('publico_alvo_ads') or '-'
        loc_str = ' / '.join([p for p in [cidade, uf, pais] if p]) or '-'
        li_link = f'<a href="{_esc(li_url)}" target="_blank">{_esc(li_url[:60])}</a>' if li_url and 'linkedin' in li_url.lower() else '-'
        approx_note = ''
        if r.get('senioridade_aproximada') and str(r['senioridade_aproximada']).lower() in ('true', '1'):
            approx_note = ' <span style="color:#d97706;font-size:.72rem;">(aproximada)</span>'
        st.markdown(f'''<div class="tap-detail"><div class="tap-detail-grid">
  <div><div class="tap-dl">Nome</div><div class="tap-dv">{_esc(nome)}</div></div>
  <div><div class="tap-dl">Cargo</div><div class="tap-dv">{_esc(cargo)}</div></div>
  <div><div class="tap-dl">Job Title</div><div class="tap-dv">{_esc(job_title) if job_title else "-"}</div></div>
  <div><div class="tap-dl">Empresa</div><div class="tap-dv">{_esc(company)}</div></div>
  <div><div class="tap-dl">Senioridade</div><div class="tap-dv">{_esc(sen)}{approx_note}</div></div>
  <div><div class="tap-dl">Expertise</div><div class="tap-dv">{_esc(expertise)}</div></div>
  <div><div class="tap-dl">Segmento</div><div class="tap-dv">{_esc(segmento)}</div></div>
  <div><div class="tap-dl">Area de mercado</div><div class="tap-dv">{_esc(mercado)}</div></div>
  <div><div class="tap-dl">Localizacao</div><div class="tap-dv">{_esc(loc_str)}</div></div>
  <div><div class="tap-dl">Publico alvo</div><div class="tap-dv">{_esc(publico)}</div></div>
  <div><div class="tap-dl">Email</div><div class="tap-dv">{_esc(email)}</div></div>
  <div><div class="tap-dl">Telefone</div><div class="tap-dv">{_esc(tel)}</div></div>
  <div><div class="tap-dl">LinkedIn</div><div class="tap-dv">{li_link}</div></div>
  <div><div class="tap-dl">Recrutador</div><div class="tap-dv">{_esc(recrutador)}</div></div>
</div></div>''', unsafe_allow_html=True)
