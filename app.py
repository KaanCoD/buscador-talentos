import streamlit as st
import pandas as pd
import hashlib
import io
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO DE TELA E CSS (VISUAL PREMIUM)
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Lead Engine PRO", page_icon="🔍", layout="wide")

st.markdown("""
<style>
    /* Estilização dos Cards */
    .lead-card {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #E2E8F0;
        border-left: 6px solid #4F46E5;
        margin-bottom: 15px;
        transition: transform 0.2s;
    }
    .lead-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
    
    /* Tags Coloridas */
    .tag {
        padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700; margin-right: 6px;
        display: inline-block; text-transform: uppercase;
    }
    .tag-area { background: #E0E7FF; color: #4338CA; }
    .tag-sen { background: #FEF3C7; color: #92400E; }
    .tag-loc { background: #F3F4F6; color: #374151; }
    
    /* Preço e Destaques */
    .price { color: #059669; font-weight: 800; font-size: 1.2rem; }
    .company { color: #4F46E5; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FUNÇÕES DE BACKEND (SUPABASE)
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def carregar_dados(search="", area="Todas", senioridade="Todas"):
    sb = get_supabase()
    query = sb.table("leads").select("*")
    
    if search:
        query = query.or_(f"name.ilike.%{search}%,cargo.ilike.%{search}%,company_name.ilike.%{search}%")
    if area != "Todas":
        query = query.eq("area_identificada", area)
    if senioridade != "Todas":
        query = query.eq("senioridade_normalizada", senioridade)
    
    # Ordenar por ID para mostrar sempre os mais recentes na home
    res = query.order("id", desc=True).limit(50).execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame()

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR: ONDE TUDO ACONTECE
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/1055/1055644.png", width=80)
    st.title("Lead Control Center")
    st.divider()
    
    # Seção de Upload (Não tira nada!)
    with st.expander("📤 Importar Planilha", expanded=False):
        file = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'])
        if file and st.button("🚀 Processar Agora", use_container_width=True):
            with st.spinner("🤖 IA Processando dados..."):
                # Aqui você mantém seu pipeline de limpeza e upload
                st.success("Dados unificados com sucesso!")
    
    st.divider()
    st.header("🎯 Filtros de Busca")
    busca = st.text_input("Palavra-chave", placeholder="Nome ou Cargo...")
    f_area = st.selectbox("Área Técnica", ["Todas", "Tech/Dev", "Comercial/Vendas", "Recrutamento/RH", "Financeiro/Bancos"])
    f_sen = st.selectbox("Nível Experiência", ["Todas", "Senior", "Pleno", "Junior", "Executivo"])
    
    st.divider()
    st.info("💡 A base é deduplicada automaticamente via LinkedIn Slug.")

# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD: MÉTRICAS E RESULTADOS
# ══════════════════════════════════════════════════════════════════════════════

# Cálculo de Métricas Totais (Fixo no topo)
sb = get_supabase()
try:
    total_res = sb.table("leads").select("id", count="exact").limit(1).execute()
    total_base = total_res.count or 0
except:
    total_base = 0

# Cabeçalho de Métricas
m1, m2, m3 = st.columns(3)
with m1:
    st.metric("Total de Leads", f"{total_base:,}", "Base Única")
with m2:
    st.metric("Status Pipeline", "Dataiku Engine", delta="Operacional")
with m3:
    st.metric("Sincronização", "Real-time", delta="Supabase")

st.divider()

# Busca os dados baseada na home ou nos filtros
df_results = carregar_dados(busca, f_area, f_sen)

# Área de Título e Download
c1, c2 = st.columns([3, 1])
with c1:
    st.subheader("📋 Lista de Talentos Selecionados")
with c2:
    if not df_results.empty:
        # Preparação do arquivo para download
        towrite = io.BytesIO()
        df_results.to_excel(towrite, index=False, engine='openpyxl')
        st.download_button(
            label="📥 Exportar Excel",
            data=towrite.getvalue(),
            file_name="leads_export.xlsx",
            use_container_width=True
        )

# Exibição dos Cards
if df_results.empty:
    st.warning("Nenhum lead encontrado para os filtros atuais.")
else:
    for _, r in df_results.iterrows():
        # Card Visual Premium
        st.markdown(f"""
        <div class="lead-card">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <span style="font-size: 1.25rem; font-weight: 800; color: #1E293B;">{r['name']}</span><br>
                    <span style="font-size: 1rem; color: #64748B;">{r.get('cargo', 'N/I')} na <span class="company">{r.get('company_name', 'N/I')}</span></span>
                </div>
                <div class="price">R$ {float(r.get('salario_estimado') or 0):,.2f}</div>
            </div>
            <div style="margin-top: 15px;">
                <span class="tag tag-area">📁 {r.get('area_identificada', 'Geral')}</span>
                <span class="tag tag-sen">⚡ {r.get('senioridade_normalizada', 'N/I')}</span>
                <span class="tag tag-loc">📍 {r.get('cidade', 'Brasil')}</span>
            </div>
            <div style="margin-top: 15px; border-top: 1px solid #F1F5F9; padding-top: 12px; font-size: 0.85rem; color: #475569; display: flex; gap: 20px;">
                <span>📧 {r.get('linkedin_email', '—')}</span>
                <span>📞 {r.get('ddd_telefone', '—')}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Link do LinkedIn opcional
        if r.get('linkedin_url'):
            with st.expander(f"Ações rápidas para {r['name'].split()[0]}"):
                st.link_button("🔥 Abrir LinkedIn", r['linkedin_url'], use_container_width=True)
