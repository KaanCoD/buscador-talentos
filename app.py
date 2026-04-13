import streamlit as st
import pandas as pd
import hashlib
import io
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# ESTILO E CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Lead Engine PRO", page_icon="🔍", layout="wide")

st.markdown("""
<style>
    .lead-card {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #E2E8F0;
        border-left: 6px solid #4F46E5;
        margin-bottom: 15px;
    }
    .tag {
        padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 700; margin-right: 6px;
        display: inline-block;
    }
    .tag-area { background: #E0E7FF; color: #4338CA; }
    .tag-sen { background: #FEF3C7; color: #92400E; }
    .price { color: #059669; font-weight: 800; font-size: 1.2rem; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FUNÇÕES DE BANCO DE DADOS
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
    
    res = query.order("id", desc=True).limit(50).execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame()

# Função para DELETAR TUDO
def limpar_banco_total():
    sb = get_supabase()
    # No Supabase, para deletar tudo sem filtros, fazemos um match que pegue tudo
    try:
        sb.table("leads").delete().neq("id", 0).execute()
        return True
    except Exception as e:
        st.error(f"Erro ao limpar banco: {e}")
        return False

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR: OPERAÇÕES E FILTROS
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("⚙️ Painel de Gestão")
    
    # Seção de Importação
    with st.expander("📤 Importar Novos Dados"):
        file = st.file_uploader("Upload", type=['csv', 'xlsx'])
        if file and st.button("🚀 Processar"):
            with st.spinner("Integrando leads..."):
                # Seu pipeline de upload aqui
                st.success("Carga finalizada!")
                st.rerun()

    st.divider()
    
    # ZONA DE PERIGO (O Botão de Excluir que você pediu)
    with st.expander("⚠️ Zona de Perigo"):
        st.warning("Atenção: Esta ação é irreversível e apagará todos os leads do Supabase.")
        confirmar = st.text_input("Digite 'EXCLUIR' para confirmar")
        if st.button("🗑️ APAGAR TODA A BASE"):
            if confirmar == "EXCLUIR":
                with st.spinner("Limpando banco de dados..."):
                    if limpar_banco_total():
                        st.success("Base de dados resetada!")
                        st.cache_resource.clear()
                        st.rerun()
            else:
                st.error("Confirmação incorreta.")

    st.divider()
    st.header("🎯 Filtros")
    busca = st.text_input("Busca Global", placeholder="Nome, Empresa...")
    f_area = st.selectbox("Área", ["Todas", "Tech/Dev", "Comercial/Vendas", "Recrutamento/RH"])
    f_sen = st.selectbox("Senioridade", ["Todas", "Senior", "Pleno", "Junior"])

# ══════════════════════════════════════════════════════════════════════════════
# CONTEÚDO PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
sb = get_supabase()
total_leads = 0
try:
    count_res = sb.table("leads").select("id", count="exact").limit(1).execute()
    total_leads = count_res.count or 0
except: pass

# Métricas Topo
m1, m2, m3 = st.columns(3)
m1.metric("Leads Totais", f"{total_leads:,}")
m2.metric("Motor de Dados", "Dataiku Engine")
m3.metric("Infra", "Supabase", delta="Online")

st.divider()

df_results = carregar_dados(busca, f_area, f_sen)

# Cabeçalho e Exportação
c1, c2 = st.columns([3, 1])
with c1:
    st.subheader(f"📋 Resultados ({len(df_results)})")
with c2:
    if not df_results.empty:
        towrite = io.BytesIO()
        df_results.to_excel(towrite, index=False)
        st.download_button("📥 Baixar Planilha", towrite.getvalue(), "leads.xlsx", use_container_width=True)

# Cards de Leads
if df_results.empty:
    st.info("O banco de dados está vazio ou nenhum lead atende aos filtros.")
else:
    for _, r in df_results.iterrows():
        st.markdown(f"""
        <div class="lead-card">
            <div style="display: flex; justify-content: space-between;">
                <div>
                    <span style="font-size: 1.2rem; font-weight: 800;">{r['name']}</span><br>
                    <span style="color: #64748B;">{r.get('cargo')} @ <b>{r.get('company_name')}</b></span>
                </div>
                <div class="price">R$ {float(r.get('salario_estimado') or 0):,.2f}</div>
            </div>
            <div style="margin-top: 15px;">
                <span class="tag tag-area">📁 {r.get('area_identificada')}</span>
                <span class="tag tag-sen">⚡ {r.get('senioridade_normalizada')}</span>
                <span class="tag tag-loc">📍 {r.get('cidade')}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
