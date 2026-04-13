import streamlit as st
import pandas as pd
import hashlib
import io
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# ESTILO E CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Lead Engine PRO", page_icon="📈", layout="wide")

st.markdown("""
<style>
    .lead-card {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #E2E8F0;
        border-left: 6px solid #4F46E5;
        margin-bottom: 15px;
        transition: transform 0.2s;
    }
    .lead-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
    .tag {
        padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; margin-right: 5px;
    }
    .tag-area { background: #DBEAFE; color: #1E40AF; }
    .tag-sen { background: #FEF3C7; color: #92400E; }
    .tag-loc { background: #F3F4F6; color: #374151; }
    .price { color: #059669; font-weight: bold; font-size: 1.1rem; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FUNÇÕES DE DADOS
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def carregar_dados_filtrados(search="", area="Todas", senioridade="Todas"):
    sb = get_supabase()
    query = sb.table("leads").select("*")
    
    if search:
        query = query.or_(f"name.ilike.%{search}%,cargo.ilike.%{search}%,company_name.ilike.%{search}%")
    if area != "Todas":
        query = query.eq("area_identificada", area)
    if senioridade != "Todas":
        query = query.eq("senioridade_normalizada", senioridade)
        
    res = query.limit(100).execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame()

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR: UPLOAD & FILTROS
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("⚙️ Operações")
    
    # Seção de Upload
    with st.expander("📤 Subir Novos Leads"):
        file = st.file_uploader("Arraste aqui", type=['csv', 'xlsx'])
        if file and st.button("Processar Planilha"):
            with st.spinner("🤖 Rodando Data Prep..."):
                # Aqui você chama a função de pipeline que criamos antes
                st.success("Dados unificados!")
    
    st.divider()
    st.header("🎯 Filtros")
    busca = st.text_input("Busca por Nome/Cargo")
    
    # Filtros que poderiam vir do banco, mas vamos fixar os principais do seu Dataiku
    f_area = st.selectbox("Área", ["Todas", "Tech/Dev", "Comercial/Vendas", "Recrutamento/RH", "Financeiro/Bancos"])
    f_sen = st.selectbox("Senioridade", ["Todas", "Senior", "Pleno", "Junior", "Executivo"])

# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
df_result = carregar_dados_filtrados(busca, f_area, f_sen)

# Título e Botão de Download
c1, c2 = st.columns([3, 1])
with c1:
    st.title("🔍 Explorador de Leads")
with c2:
    if not df_result.empty:
        # Gerar Excel em memória para download
        towrite = io.BytesIO()
        df_result.to_excel(towrite, index=False, engine='openpyxl')
        towrite.seek(0)
        st.download_button(
            label="📥 Baixar Planilha",
            data=towrite,
            file_name="leads_filtrados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# Exibição dos Cards
if df_result.empty:
    st.info("Nenhum lead encontrado com esses filtros.")
else:
    st.write(f"Exibindo **{len(df_result)}** leads encontrados.")
    
    for _, r in df_result.iterrows():
        # Layout do Card
        st.markdown(f"""
        <div class="lead-card">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <span style="font-size: 1.2rem; font-weight: bold; color: #1E293B;">{r['name']}</span><br>
                    <span style="color: #64748B;">{r['cargo']} @ <b>{r['company_name']}</b></span>
                </div>
                <div class="price">R$ {float(r.get('salario_estimado') or 0):,.2f}</div>
            </div>
            <div style="margin-top: 12px;">
                <span class="tag tag-area">📁 {r.get('area_identificada', 'Geral')}</span>
                <span class="tag tag-sen">⚡ {r.get('senioridade_normalizada', 'N/I')}</span>
                <span class="tag tag-loc">📍 {r.get('cidade', 'Brasil')}</span>
            </div>
            <div style="margin-top: 15px; border-top: 1px solid #F1F5F9; padding-top: 10px; font-size: 0.85rem; color: #475569;">
                📧 {r.get('linkedin_email', 'N/A')} &nbsp;&nbsp; | &nbsp;&nbsp; 📞 {r.get('ddd_telefone', 'N/A')}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Botão discreto para o LinkedIn
        if r.get('linkedin_url'):
            st.link_button(f"Abrir LinkedIn de {r['name'].split()[0]}", r['linkedin_url'], icon="🔗")
