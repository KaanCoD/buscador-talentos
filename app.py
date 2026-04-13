import streamlit as st
import pandas as pd
import re
import unicodedata
import hashlib
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES E INTELIGÊNCIA DE MAPEAMENTO
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Dataiku Centralizador", page_icon="🚀", layout="wide")

TABLE = "leads"
BATCH_SIZE = 600

# Palavras-chave para identificar colunas automaticamente, não importa o nome
AUTO_MAP_KEYWORDS = {
    'name': ['nome', 'name', 'full name', 'lead', 'contato'],
    'linkedin_email': ['email', 'e-mail', 'mail', 'linkedinemail'],
    'ddd_telefone': ['tel', 'phone', 'celular', 'whatsapp', 'mobile', 'ddd'],
    'linkedin_url': ['url', 'linkedin', 'link', 'perfil'],
    'cargo': ['cargo', 'job', 'role', 'occupation', 'titulo', 'title'],
    'company_name': ['empresa', 'company', 'organization', 'trabalho'],
    'nivel_senioridade': ['senioridade', 'seniority', 'nivel', 'level']
}

REGRAS = {
    'areas': {
        'engenharia de software': 'Tech/Dev', 'desenvolvedor': 'Tech/Dev', 'dev': 'Tech/Dev',
        'vendas': 'Comercial/Vendas', 'comercial': 'Comercial/Vendas', 'sales': 'Comercial/Vendas',
        'rh': 'Recrutamento/RH', 'recrutador': 'Recrutamento/RH', 'talent': 'Recrutamento/RH'
    },
    'salarios': {
        'Tech/Dev': {'Senior': 12000, 'Pleno': 8500, 'Junior': 4500},
        'Comercial/Vendas': {'Senior': 10000, 'Pleno': 6500, 'Junior': 3500},
        'default': {'Senior': 9000, 'Pleno': 6000, 'Junior': 4500}
    }
}

# ══════════════════════════════════════════════════════════════════════════════
# MOTOR DE CONEXÃO
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def count_leads():
    try: return get_supabase().table(TABLE).select("id", count="exact").limit(1).execute().count or 0
    except: return 0

# ══════════════════════════════════════════════════════════════════════════════
# LÓGICA DE TRATAMENTO "UNIVERSAL"
# ══════════════════════════════════════════════════════════════════════════════

def identificar_colunas(df_cols):
    """Mapeia colunas da planilha bagunçada para o nosso padrão"""
    mapping = {}
    for padrao, keywords in AUTO_MAP_KEYWORDS.items():
        for col in df_cols:
            if any(k in col.lower() for k in keywords):
                mapping[padrao] = col
                break
    return mapping

def normalizar_texto(txt):
    if pd.isna(txt): return None
    return str(txt).strip()

def extrair_slug(url, email, nome):
    """Cria um ID Único (Slug) baseado no que estiver disponível para evitar duplicatas"""
    if pd.notna(url) and 'linkedin.com/in/' in str(url):
        return str(url).split('/in/')[-1].split('?')[0].rstrip('/')
    if pd.notna(email):
        return hashlib.md5(str(email).lower().strip().encode()).hexdigest()
    return hashlib.md5(str(nome).lower().strip().encode()).hexdigest()

def aplicar_inteligencia_dataiku(row):
    """Aplica as receitas de Área, Senioridade e Salário"""
    # 1. Senioridade
    cargo = str(row.get('cargo', '')).lower()
    nivel = str(row.get('nivel_senioridade', '')).lower()
    texto_busca = cargo + " " + nivel
    
    if 'senior' in texto_busca or 'sênior' in texto_busca or 'esp' in texto_busca: sen = 'Senior'
    elif 'pleno' in texto_busca: sen = 'Pleno'
    elif 'jr' in texto_busca or 'junior' in texto_busca or 'estagi' in texto_busca: sen = 'Junior'
    else: sen = 'Nao Identificado'

    # 2. Área
    area = "Outros/Nao Identificado"
    for termo, a in REGRAS['areas'].items():
        if termo in texto_busca:
            area = a
            break
            
    # 3. Financeiro
    tabela = REGRAS['salarios'].get(area, REGRAS['salarios']['default'])
    salario = tabela.get(sen, 4500)
    
    return pd.Series([sen, area, salario, salario * 13.3])

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE DE EXECUÇÃO
# ══════════════════════════════════════════════════════════════════════════════

def processar_planilha_universal(file, progress_cb):
    sb = get_supabase()
    progress_cb(10, "📖 Lendo arquivo...")
    
    # Suporta CSV (com vírgula ou tab) e Excel
    try:
        df = pd.read_csv(file, sep=None, engine='python', dtype=str)
    except:
        df = pd.read_excel(file, dtype=str)

    progress_cb(25, "🔍 Identificando colunas automaticamente...")
    mapa_detectado = identificar_colunas(df.columns)
    
    # Criar novo DF padronizado
    df_clean = pd.DataFrame()
    for padrao, col_original in mapa_detectado.items():
        df_clean[padrao] = df[col_original].apply(normalizar_texto)

    # Preencher colunas que faltarem com None
    for col in AUTO_MAP_KEYWORDS.keys():
        if col not in df_clean.columns: df_clean[col] = None

    progress_cb(40, "🧠 Aplicando Inteligência (Receitas Dataiku)...")
    df_clean[['senioridade_normalizada', 'area_identificada', 'salario_estimado', 'custo_total']] = df_clean.apply(aplicar_inteligencia_dataiku, axis=1)
    
    progress_cb(55, "🛡️ Removendo duplicatas...")
    df_clean['linkedin_slug'] = df_clean.apply(lambda r: extrair_slug(r['linkedin_url'], r['linkedin_email'], r['name']), axis=1)
    df_clean = df_clean.drop_duplicates(subset=['linkedin_slug'])

    # Conversão para dicionário para o Supabase
    records = df_clean.to_dict(orient='records')
    for r in records:
        # Garantir que números sejam floats e nans sejam Nones
        r['salario_estimado'] = float(r['salario_estimado'])
        r['custo_total'] = float(r['custo_total'])
        for k, v in r.items():
            if pd.isna(v): r[k] = None

    progress_cb(70, f"🚀 Subindo {len(records)} leads para o banco central...")
    
    sucesso = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            sb.table(TABLE).upsert(batch, on_conflict='linkedin_slug').execute()
            sucesso += len(batch)
        except:
            for r in batch:
                try: sb.table(TABLE).upsert(r, on_conflict='linkedin_slug').execute(); sucesso += 1
                except: pass
        progress_cb(70 + int((i/len(records))*30), f"Sincronizando... {sucesso} leads")

    return sucesso

# ══════════════════════════════════════════════════════════════════════════════
# INTERFACE
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("📥 Centralizador")
    st.info("Jogue qualquer planilha. O sistema tentará identificar Nome, Email, Tel e LinkedIn sozinho.")
    
    st.metric("Base Central", f"{count_leads():,}")
    
    file = st.file_uploader("Upload", type=['csv', 'xlsx'])
    if file and st.button("🚀 Processar e Unificar", use_container_width=True):
        bar = st.progress(0); msg = st.empty()
        def up(p, m): bar.progress(p/100); msg.text(m)
        try:
            total = processar_planilha_universal(file, up)
            st.success(f"Finalizado! {total} leads adicionados/atualizados.")
            st.cache_data.clear()
        except Exception as e: st.error(f"Erro no processamento: {e}")

st.title("🔍 Busca na Base Unificada")
busca = st.text_input("Busca rápida (Nome, Cargo, Email ou Empresa)")

if busca:
    sb = get_supabase()
    res = sb.table(TABLE).select("*").or_(f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%,linkedin_email.ilike.%{busca}%").limit(100).execute()
    
    if res.data:
        st.dataframe(pd.DataFrame(res.data)[['name', 'cargo', 'company_name', 'area_identificada', 'senioridade_normalizada', 'salario_estimado']], use_container_width=True)
    else:
        st.warning("Nada encontrado.")
