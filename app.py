```python
"""
🔍 Buscador de Talentos — Streamlit + Supabase
Pipeline: Upload CSV/XLSX → Limpeza → Normalização → Deduplicação → Supabase → Busca
Suporta 1M+ leads
"""
import streamlit as st
import pandas as pd
import re
import unicodedata
import hashlib
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
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

# ══════════════════════════════════════════════════════════════════════════════
# SUPABASE CONNECTION
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


def ensure_table_exists():
    sb = get_supabase()
    sql = """
    CREATE TABLE IF NOT EXISTS leads (
        id BIGSERIAL PRIMARY KEY,
        name TEXT, cargo TEXT, occupation TEXT, job_title TEXT, company_name TEXT,
        expertise TEXT, nivel_senioridade TEXT, senioridade_normalizada TEXT,
        segmento_empresa TEXT, segmento_mercado TEXT, linkedin_email TEXT,
        ddd_telefone TEXT, linkedin_url TEXT, cidade TEXT, uf TEXT, estado TEXT,
        pais TEXT, publico_alvo_ads TEXT, recrutador TEXT, sales_navigator_id TEXT,
        senioridade_aproximada BOOLEAN DEFAULT FALSE, linkedin_slug TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_leads_linkedin_slug ON leads (linkedin_slug);
    CREATE INDEX IF NOT EXISTS idx_leads_sales_navigator_id ON leads (sales_navigator_id);
    CREATE INDEX IF NOT EXISTS idx_leads_linkedin_email ON leads (linkedin_email);
    CREATE INDEX IF NOT EXISTS idx_leads_senioridade ON leads (senioridade_normalizada);
    CREATE INDEX IF NOT EXISTS idx_leads_expertise ON leads (expertise);
    CREATE INDEX IF NOT EXISTS idx_leads_uf ON leads (uf);
    CREATE INDEX IF NOT EXISTS idx_leads_estado ON leads (estado);
    """
    sb.postgrest.auth(sb.options.headers.get("apikey"))
    sb.rpc("exec_sql", {"query": sql}).execute()


def init_table_via_rpc():
    try:
        ensure_table_exists()
        return True
    except Exception:
        return False


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
                [f"estado.ilike.{e}" for e in filters['estados']]
            )
            q = q.or_(or_parts)
    if query.strip():
        terms = [t for t in query.lower().split() if len(t) > 2]
        for term in terms:
            or_parts = ",".join([
                f"cargo.ilike.%{term}%", f"occupation.ilike.%{term}%",
                f"job_title.ilike.%{term}%", f"expertise.ilike.%{term}%",
                f"company_name.ilike.%{term}%", f"name.ilike.%{term}%",
                f"cidade.ilike.%{term}%", f"uf.ilike.%{term}%",
                f"estado.ilike.%{term}%", f"segmento_mercado.ilike.%{term}%",
            ])
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
                progress_callback(min(pct, 99), f"📤 Enviando... {inserted:,}/{total:,}")
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
                    errors.append(f"Amostra do registro: {sample}")
                if len(errors) >= 6:
                    break
    if errors:
        st.session_state['pipeline_errors'] = errors
    return inserted


# ══════════════════════════════════════════════════════════════════════════════
# FUNÇÕES AUXILIARES DO PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def strip_accents(s):
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(s))
        if unicodedata.category(c) != 'Mn'
    ).lower().strip()


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


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 1 — LIMPEZA DE LOCALIZAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
def limpar_localizacao(df):
    df = df.copy()
    for col in ['Cidade', 'UF', 'Estado', 'Pais', 'location']:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].astype(str).replace('nan', '')
    uf_map = {'Federal District': 'Brasilia', 'Brasília': 'Brasilia'}
    for old, new in uf_map.items():
        df.loc[df['UF'] == old, 'UF'] = new
    cidade_map = {'Federal District': 'Brasilia', 'Distrito Federal': 'Brasilia', 'Brasília': 'Brasilia'}
    for old, new in cidade_map.items():
        df.loc[df['Cidade'] == old, 'Cidade'] = new
    cidade_to_uf = {
        'Planaltina': 'Distrito Federal', 'Brasilia': 'Distrito Federal',
        'Taguatinga': 'Distrito Federal', 'Ceilândia': 'Distrito Federal',
        'Gama': 'Distrito Federal',
    }
    for cidade, uf in cidade_to_uf.items():
        df.loc[df['Cidade'] == cidade, 'UF'] = uf
    cidade_to_estado = {
        'Sao Paulo': 'Sao Paulo', 'Osasco': 'Sao Paulo',
        'Guarulhos': 'Sao Paulo', 'San Pablo': 'Filipinas',
    }
    for cidade, estado in cidade_to_estado.items():
        df.loc[df['Cidade'] == cidade, 'Estado'] = estado
    estado_map = {'San Pablo': 'Sao Paulo', 'Sao Paulo': 'SP', 'Brasil': 'DF', 'Minnesota': 'EUA'}
    for old, new in estado_map.items():
        df.loc[df['Estado'] == old, 'Estado'] = new
    df.loc[df['UF'] == 'San Pablo', 'Estado'] = 'Filipinas'
    df.loc[df['UF'] == 'Distrito Federal', 'Estado'] = 'DF'
    df.loc[df['location'] == 'Brésil', 'Pais'] = 'Brasil'
    df.loc[df['location'] == 'Brésil', 'location'] = 'Brasil'
    df.loc[df['location'] == 'Brazil', 'location'] = 'Brasil'
    mask_exterior = (df['Pais'].str.strip() != '') & (df['Pais'] != 'Brasil') & (df['Pais'] != 'nan')
    df.loc[mask_exterior, 'Estado'] = 'Exterior'
    return df


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 2 — NORMALIZAÇÃO DE SENIORIDADE
# ══════════════════════════════════════════════════════════════════════════════
CAT18_SEN_MAP = {
    'Liderança/Executivo': 'Lideranca/Executivo', 'Liderança': 'Lideranca/Executivo',
    'Sênior/Especialista': 'Senior/Especialista', 'Sênior': 'Senior/Especialista',
    'Especialista': 'Senior/Especialista', 'Pleno': 'Pleno',
    'Júnior': 'Junior/Estagio', 'Júnior/Trainee': 'Junior/Estagio',
    'Estágio/Trainee': 'Junior/Estagio', 'Estagiário': 'Junior/Estagio',
    'Coordenação': 'Lideranca/Executivo', 'Gerência': 'Lideranca/Executivo',
}

EXPERTISE_SEN_MAP = {
    'Dados e BI': 'Pleno', 'Engenharia de software': 'Pleno',
    'Agile': 'Senior/Especialista', 'Recrutamento e seleção': 'Pleno',
    'Operações e processos': 'Pleno', 'Finanças e controladoria': 'Senior/Especialista',
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
    if prefix18 in ('Executivo/Dono', 'Gestão/Liderança'): return 'Lideranca/Executivo'
    return 'Nao Identificado'


def _build_lookups(dataframe):
    if 'Nivel_Senioridade' not in dataframe.columns:
        return {}, {}, {}, {}, {}, {}
    has_nivel = dataframe['Nivel_Senioridade'].notna() & \
                (~dataframe['Nivel_Senioridade'].astype(str).isin(['', 'nan', 'NaN']))
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
    if prefix18 in ('Executivo/Dono', 'Gestão/Liderança'): return 'Lideranca/Executivo', False
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
    for col in ['Nivel_Senioridade', 'Cargo', 'job_title', 'Categoria_Cargo',
                'segmento_mercado', 'Expertise']:
        if col not in df.columns:
            df[col] = ''
    lookups = _build_lookups(df)
    df['senioridade_normalizada'] = df.apply(
        lambda r: _inferir_exact(
            r.get('Nivel_Senioridade', ''), r.get('Cargo', ''),
            r.get('job_title', ''), r.get('Categoria_Cargo', '')
        ), axis=1
    )
    df['senioridade_aproximada'] = False
    mask_nao = df['senioridade_normalizada'] == 'Nao Identificado'
    if mask_nao.any():
        rescued = df[mask_nao].apply(
            lambda r: _rescue(
                r.get('Cargo', ''), r.get('job_title', ''),
                r.get('Categoria_Cargo', ''), r.get('segmento_mercado', ''),
                r.get('Expertise', ''), lookups
            ), axis=1
        )
        df.loc[mask_nao, 'senioridade_normalizada'] = rescued.apply(lambda x: x[0])
        df.loc[mask_nao, 'senioridade_aproximada'] = rescued.apply(lambda x: x[1])
    return df


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 3 — DEDUPLICAÇÃO LOCAL
# ══════════════════════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE COMPLETO
# ══════════════════════════════════════════════════════════════════════════════
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
    if progress_callback: progress_callback(5, "📂 Lendo arquivo...")
    name = uploaded_file.name.lower()
    if name.endswith('.csv'):
        df = pd.read_csv(uploaded_file, dtype=str)
        known_cols = {'name', 'linkedinEmail', 'Cargo', 'linkedinUrl', 'company_name', 'Cidade'}
        if not known_cols.intersection(set(df.columns)):
            if progress_callback: progress_callback(8, "📂 CSV sem cabeçalho detectado, aplicando schema...")
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
    for col in ['Cidade', 'UF', 'Estado', 'Pais', 'location', 'Nivel_Senioridade',
                'Cargo', 'job_title', 'Categoria_Cargo', 'segmento_mercado',
                'Expertise', 'salesNavigatorId', 'linkedinUrl', 'linkedinEmail']:
        if col not in df.columns:
            df[col] = ''
    if progress_callback: progress_callback(20, "🧹 Limpando localização...")
    df = limpar_localizacao(df)
    if progress_callback: progress_callback(40, "🎯 Normalizando senioridade...")
    df = normalizar_senioridade(df)
    if progress_callback: progress_callback(60, "🔍 Deduplicando...")
    df, antes, depois = deduplicar_local(df)
    if progress_callback: progress_callback(75, f"📤 Enviando {depois:,} leads pro banco...")
    df_out = pd.DataFrame()
    for orig, dest in COL_MAP.items():
        if orig in df.columns:
            df_out[dest] = df[orig].astype(str).replace({'nan': None, 'None': None, '': None})
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
    records = df_out.to_dict(orient='records')
    for r in records:
        for k, v in list(r.items()):
            if v in ('nan', 'None', '', 'none', 'NaN'):
                r[k] = None
        sa = r.get('senioridade_aproximada')
        if sa is not None:
            r['senioridade_aproximada'] = str(sa).lower() in ('true', '1', 'yes')
        else:
            r['senioridade_aproximada'] = False
    inserted = insert_leads_batch(records, progress_callback)
    if progress_callback: progress_callback(100, f"✅ {inserted:,} leads salvos!")
    return {
        'arquivo': uploaded_file.name,
        'linhas_raw': total_raw,
        'antes_dedup': antes,
        'depois_dedup': depois,
        'inseridos': inserted,
    }


# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        padding: 1.2rem; border-radius: 12px; border: 1px solid #475569; text-align: center;
    }
    .metric-card h3 { color: #94a3b8; font-size: 0.85rem; margin: 0; }
    .metric-card p { color: #f1f5f9; font-size: 1.8rem; font-weight: 700; margin: 0.3rem 0 0 0; }
    .lead-card {
        background: var(--color-background-secondary); border: 0.5px solid var(--color-border-tertiary);
        border-radius: 10px; padding: 1rem 1.2rem; margin-bottom: 0.6rem;
    }
    .lead-card:hover { border-color: var(--color-border-info); }
    .lead-name { font-size: 1.05rem; font-weight: 600; color: var(--color-text-primary); }
    .lead-cargo { font-size: 0.85rem; color: var(--color-text-secondary); }
    .lead-company { font-size: 0.85rem; color: var(--color-text-tertiary); }
    .lead-tags { margin-top: 0.4rem; }
    .lead-tag {
        display: inline-block; background: var(--color-background-tertiary);
        color: var(--color-text-secondary); padding: 2px 8px; border-radius: 4px;
        font-size: 0.75rem; margin-right: 4px; margin-top: 4px;
    }
    .lead-tag.senior { background: var(--color-background-info); color: var(--color-text-info); }
    .lead-tag.location { background: var(--color-background-success); color: var(--color-text-success); }
</style>
""", unsafe_allow_html=True)


with st.sidebar:
    st.markdown("## 📤 Importar Planilha")
    total = count_leads()
    if total > 0:
        st.success(f"Banco ativo: **{total:,}** leads")
    else:
        st.info("Banco vazio — suba sua primeira planilha!")
    st.markdown("---")
    uploaded = st.file_uploader("Arraste CSV ou XLSX aqui", type=['csv', 'xlsx', 'xls'], label_visibility="collapsed")
    if uploaded and st.button("🚀 Processar e Salvar", type="primary", use_container_width=True):
        progress_bar = st.progress(0)
        status_text = st.empty()
        spinner_placeholder = st.empty()
        if 'cancelar_upload' not in st.session_state:
            st.session_state.cancelar_upload = False
        stop_btn = st.button("⏹️ Parar processamento", use_container_width=True)
        if stop_btn:
            st.session_state.cancelar_upload = True
        def update_progress(pct, msg):
            progress_bar.progress(pct / 100)
            spinner_placeholder.markdown(
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<div style="width:20px;height:20px;border:3px solid var(--color-border-tertiary);'
                f'border-top:3px solid var(--color-text-info);border-radius:50%;'
                f'animation:spin 1s linear infinite;"></div>'
                f'<span style="font-size:0.85rem;color:var(--color-text-secondary);">{msg}</span>'
                f'</div>'
                f'<style>@keyframes spin {{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}</style>',
                unsafe_allow_html=True
            )
            status_text.text("")
        try:
            result = pipeline_e_salva(uploaded, update_progress)
            spinner_placeholder.empty()
            st.session_state['pipeline_success'] = (
                f"✅ **{result['arquivo']}** — "
                f"Linhas: {result['linhas_raw']:,} → "
                f"Dedup: {result['depois_dedup']:,} → "
                f"Salvos: {result['inseridos']:,}"
            )
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            import traceback
            spinner_placeholder.empty()
            st.session_state['pipeline_errors'] = [
                f"Erro: {e}",
                traceback.format_exc()
            ]
            st.rerun()
    st.markdown("---")
    st.caption("Cada planilha que você sobe passa pelo pipeline completo "
               "(limpeza → normalização → deduplicação) e é adicionada ao banco.")
    if count_leads() > 0:
        st.markdown("---")
        with st.expander("⚠️ Manutenção"):
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


st.markdown("# 🔍 Buscador de Talentos")

if st.session_state.get('pipeline_errors'):
    st.error("⚠️ Erros durante o upload:")
    for err in st.session_state['pipeline_errors'][:5]:
        st.code(err, language="text")
    if st.button("✕ Fechar erros"):
        del st.session_state['pipeline_errors']
        st.rerun()

if st.session_state.get('pipeline_success'):
    st.success(st.session_state['pipeline_success'])
    if st.button("✕ Fechar"):
        del st.session_state['pipeline_success']
        st.rerun()

total = count_leads()
if total == 0:
    st.markdown("""
    <div style="text-align: center; padding: 4rem 2rem;">
        <h2 style="color: var(--color-text-tertiary);">Nenhum lead no banco</h2>
        <p style="color: var(--color-text-secondary); font-size: 1.1rem;">
            Use a barra lateral para subir sua primeira planilha de scraps.
        </p>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f'<div class="metric-card"><h3>Total leads</h3><p>{total:,}</p></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-card"><h3>Banco</h3><p>Supabase</p></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><h3>Status</h3><p>Online</p></div>', unsafe_allow_html=True)

st.markdown("")

@st.cache_data(ttl=300)
def cached_filter_options():
    return get_filter_options()

options = cached_filter_options()

with st.expander("🎛️ Filtros", expanded=True):
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
        ESTADOS_BR = sorted(['AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT',
                             'PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO'])
        fil_estados = st.multiselect("Estado (UF)", ESTADOS_BR)

consulta = st.text_input("🔎 Busca livre", placeholder='Ex: "head de produto SP" ou "engenheiro sênior"')

filters = {}
if fil_senioridade: filters['senioridade_normalizada'] = fil_senioridade
if fil_expertise: filters['expertise'] = fil_expertise
if fil_segmento: filters['segmento_empresa'] = fil_segmento
if fil_mercado: filters['segmento_mercado'] = fil_mercado
if fil_estados: filters['estados'] = fil_estados

has_filters = bool(filters) or bool(consulta.strip())
if not has_filters:
    st.info("Use os filtros ou a busca para encontrar leads.")
    st.stop()

with st.spinner("Buscando..."):
    results = search_leads(query=consulta, filters=filters, limit=50)

st.markdown(f"**{len(results)} resultados** (mostrando até 50)")

if results:
    df_export = pd.DataFrame(results)
    cols_export = [c for c in COLUNAS_BANCO if c in df_export.columns]
    csv_data = df_export[cols_export].to_csv(index=False).encode('utf-8-sig')
    ecol1, ecol2, _ = st.columns([1, 1, 4])
    with ecol1:
        st.download_button("📥 CSV", csv_data, "resultados.csv", "text/csv")
    with ecol2:
        emails = [r.get('linkedin_email') for r in results if r.get('linkedin_email') and '@' in str(r.get('linkedin_email', ''))]
        if emails:
            st.download_button("📧 Emails", '\n'.join(emails), "emails.txt", "text/plain")

for r in results:
    nome = r.get('name') or '—'
    cargo = r.get('cargo') or '—'
    company = r.get('company_name') or '—'
    sen = r.get('senioridade_normalizada') or ''
    expertise = r.get('expertise') or ''
    cidade = r.get('cidade') or ''
    uf = r.get('uf') or r.get('estado') or ''
    email = r.get('linkedin_email') or ''
    tel = r.get('ddd_telefone') or ''
    li_url = r.get('linkedin_url') or ''
    tags_html = ''
    if sen and sen != 'Nao Identificado':
        tags_html += f'<span class="lead-tag senior">{sen}</span>'
    if expertise:
        tags_html += f'<span class="lead-tag">{expertise}</span>'
    loc_parts = [p for p in [cidade, uf] if p]
    if loc_parts:
        tags_html += f'<span class="lead-tag location">{" / ".join(loc_parts)}</span>'
    contact_html = ''
    if email and '@' in email:
        contact_html += f'<span style="color:var(--color-text-secondary); font-size:0.8rem;">📧 {email}</span> '
    if tel:
        contact_html += f'<span style="color:var(--color-text-secondary); font-size:0.8rem;">📞 {tel}</span> '
    if li_url and 'linkedin' in li_url.lower():
        contact_html += f'<a href="{li_url}" target="_blank" style="color:var(--color-text-info); font-size:0.8rem;">🔗 LinkedIn</a>'
    st.markdown(f"""
    <div class="lead-card">
        <div class="lead-name">{nome}</div>
        <div class="lead-cargo">{cargo}</div>
        <div class="lead-company">{company}</div>
        <div class="lead-tags">{tags_html}</div>
        <div style="margin-top: 0.4rem;">{contact_html}</div>
    </div>
    """, unsafe_allow_html=True)
```
