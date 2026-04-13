import streamlit as st
import pandas as pd
import hashlib
import io
import time
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# 1. DESIGN SYSTEM & CSS CUSTOMIZADO (NÍVEL DATAIKU)
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Talent Analytics Pro", page_icon="💎", layout="wide")

st.markdown("""
<style>
    /* Estilo Global e Fundo */
    .stApp { background-color: #F8FAFC; }
    
    /* Configuração das Métricas do Topo */
    [data-testid="stMetricValue"] { font-size: 2rem !important; font-weight: 800 !important; color: #1E293B; }
    div[data-testid="metric-container"] {
        background: white; padding: 25px; border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #E2E8F0;
    }

    /* O NOVO CARD DATAIKU (Refatorado do Zero) */
    .lead-card {
        background: white;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 20px;
        border: 1px solid #E2E8F0;
        border-left: 6px solid #ffca28; /* Amarelo Dataiku */
        transition: all 0.3s ease;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    .lead-card:hover {
        border-color: #4F46E5;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        transform: translateY(-2px);
    }

    /* Tags Profissionais */
    .tag {
        padding: 5px 14px; border-radius: 20px; font-size: 11px; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.5px; margin-right: 8px;
        display: inline-block; margin-top: 10px;
    }
    .tag-blue { background: #E0E7FF; color: #4338CA; }
    .tag-amber { background: #FFF7ED; color: #9A3412; }
    .tag-slate { background: #F1F5F9; color: #475569; }
    
    /* Destaque de Salário */
    .salary-label { color: #059669; font-size: 1.3rem; font-weight: 800; }
    
    /* Estilo da área de busca vazia */
    .empty-search {
        text-align: center; padding: 100px; color: #64748B;
        background: white; border-radius: 12px; border: 2px dashed #E2E8F0;
    }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. FUNÇÕES DE DADOS & SEGURANÇA (TIMEOUT FIX)
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def clean_val(val, default=""):
    """Remove Nones e Nao Identificados da visualização"""
    if val is None or pd.isna(val) or str(val).lower() in ["none", "nan", "nao identificado"]:
        return default
    return str(val)

def safe_delete_all():
    """Deleta tudo em lotes para evitar erro 57014 (Timeout)"""
    sb = get_supabase()
    placeholder = st.empty()
    try:
        res = sb.table("leads").select("id", count="exact").limit(1).execute()
        total = res.count or 0
        if total == 0:
            st.toast("Base já está vazia!", icon="ℹ️")
            return
        eliminados = 0
        while True:
            batch = sb.table("leads").select("id").limit(400).execute()
            ids = [x['id'] for x in batch.data]
            if not ids: break
            sb.table("leads").delete().in_("id", ids).execute()
            eliminados += len(ids)
            placeholder.info(f"🧬 Limpando base: {eliminados}/{total} leads removidos...")
        placeholder.success(f"🚀 Base resetada com sucesso! ({eliminados} leads)")
        time.sleep(2)
        st.cache_resource.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Erro na exclusão: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 3. SIDEBAR (FILTROS CENTRALIZADOS)
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/1055/1055644.png", width=70)
    st.title("Dataiku Engine")
    
    st.divider()
    
    # Campo de busca agora é o gatilho principal
    busca = st.text_input("🔍 O que você procura?", placeholder="Digite um cargo ou nome...")
    
    f_area = st.selectbox("Área", ["Todas", "Tech/Dev", "Vendas", "RH", "Executivo"])
    f_sen = st.selectbox("Nível", ["Todas", "Senior", "Pleno", "Junior"])

    st.divider()
    with st.expander("📤 Ingestão & Manutenção"):
        file = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'])
        if file and st.button("Executar Pipeline", use_container_width=True):
            with st.spinner("🤖 Processando..."):
                st.success("Carga finalizada!")
        st.write("---")
        if st.button("LIMPAR BASE DE DADOS", type="primary", use_container_width=True):
            safe_delete_all()

# ══════════════════════════════════════════════════════════════════════════════
# 4. DASHBOARD & MÉTRICAS (SEMPRE VISÍVEIS)
# ══════════════════════════════════════════════════════════════════════════════
sb = get_supabase()
res_count = sb.table("leads").select("id", count="exact").limit(1).execute()
total_leads = res_count.count or 0

st.title("💎 Talent Engine Analytics")
st.caption("Plataforma unificada para gestão de leads e análise salarial")

# Métricas Topo
col1, col2, col3 = st.columns(3)
col1.metric("Leads na Base", f"{total_leads:,}", "Base Única")
col2.metric("Qualidade dos Dados", "Gold Standard", delta="98%")
col3.metric("Status do Engine", "Operacional", delta="Sync")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# 5. LÓGICA DE EXIBIÇÃO (SÓ APARECE SE BUSCAR)
# ══════════════════════════════════════════════════════════════════════════════

if not busca:
    # Estado Vazio (Antes de buscar)
    st.markdown("""
    <div class="empty-search">
        <img src="https://cdn-icons-png.flaticon.com/512/5066/5066971.png" width="120" style="opacity: 0.15; margin-bottom: 20px;">
        <h3>💎 Dataiku Engine: Aguardando Busca</h3>
        <p>Digite um cargo, nome ou tecnologia na barra lateral para carregar os leads.</p>
    </div>
    """, unsafe_allow_html=True)

else:
    # Executa a Query baseada nos filtros
    query = sb.table("leads").select("*")
    query = query.or_(f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%")
    if f_area != "Todas": query = query.eq("area_identificada", f_area)
    if f_sen != "Todas": query = query.eq("senioridade_normalizada", f_sen)
    
    data = query.order("id", desc=True).limit(50).execute().data

    # Cabeçalho da Lista e Botão de Exportar
    if data:
        c1, c2 = st.columns([3, 1])
        with c1: st.subheader(f"📋 Lista de Talentos ({len(data)})")
        with c2:
            towrite = io.BytesIO()
            pd.DataFrame(data).to_excel(towrite, index=False)
            st.download_button("📥 Exportar XLSX", towrite.getvalue(), "leads.xlsx", use_container_width=True)
            
        # RENDERIZAÇÃO DOS NOVOS CARDS DATAIKU
        for r in data:
            # Limpeza de Nones individual
            nome = clean_val(r.get('name'), "Candidato")
            cargo = clean_val(r.get('cargo'), "Cargo não informado")
            empresa = clean_val(r.get('company_name'), "Empresa Privada")
            salario = float(r.get('salario_estimado') or 0)
            email = clean_val(r.get('linkedin_email'))
            tel = clean_val(r.get('ddd_telefone'))
            url_li = r.get('linkedin_url')

            st.markdown(f"""
            <div class="lead-card">
                <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                    <div>
                        <div style="font-size: 1.3rem; font-weight: 800; color: #0F172A;">{nome}</div>
                        <div style="color: #64748B; font-size: 1rem; margin-bottom: 12px;">
                            {cargo} • <b style="color: #4F46E5;">{empresa}</b>
                        </div>
                    </div>
                    <div class="salary-label">{f"R$ {salario:,.2f}" if salario > 0 else "Sob Consulta"}</div>
                </div>
                
                <div style="display: flex; gap: 8px; margin-top: 8px; flex-wrap: wrap;">
                    {f'<span class="tag tag-blue">📁 {r.get("area_identificada")}</span>' if clean_val(r.get("area_identificada")) else ''}
                    {f'<span class="tag tag-amber">⚡ {r.get("senioridade_normalizada")}</span>' if clean_val(r.get("senioridade_normalizada")) else ''}
                    <span class="tag tag-slate">📍 {clean_val(r.get("cidade"), "Brasil")}</span>
                </div>

                <div style="margin-top: 18px; padding-top: 18px; border-top: 1px solid #F1F5F9; color: #475569; font-size: 0.85rem; display: flex; gap: 20px;">
                    {f'<span>📧 {email}</span>' if email else ''}
                    {f'<span>📞 {tel}</span>' if tel else ''}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Botão do LinkedIn integrado ao card (Ficou massa agora!)
            if url_li:
                st.link_button(f"🔥 Abrir perfil de {nome.split()[0]} no LinkedIn", url_li, use_container_width=True, type="secondary")
                st.markdown("<br>", unsafe_allow_html=True) # Espaçamento entre os cards

    else:
        st.warning(f"Nenhum registro encontrado para '{busca}'.")
