import streamlit as st
import pandas as pd
import io
import time
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# 1. DESIGN SYSTEM (TOTALMENTE LIMPO)
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Talent Analytics Pro", page_icon="📈", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #F8FAFC; }
    /* Esconde elementos desnecessários do Streamlit */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    div[data-testid="metric-container"] {
        background: white; padding: 20px; border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #E2E8F0;
    }
    .lead-card {
        background: white; border-radius: 12px; padding: 24px; margin-bottom: 16px;
        border: 1px solid #E2E8F0; border-left: 6px solid #ffca28;
        transition: all 0.3s ease;
    }
    .tag {
        padding: 4px 12px; border-radius: 6px; font-size: 11px; font-weight: 700;
        text-transform: uppercase; margin-right: 8px; display: inline-block;
    }
    .tag-blue { background: #E0E7FF; color: #4338CA; }
    .tag-amber { background: #FFF7ED; color: #9A3412; }
    .tag-slate { background: #F1F5F9; color: #475569; }
    .salary-label { color: #059669; font-size: 1.2rem; font-weight: 800; }
    .empty-state { text-align: center; padding: 80px; color: #64748B; font-family: sans-serif; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. FUNÇÕES CORE
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def clean_val(val, default=""):
    if val is None or pd.isna(val) or str(val).lower() in ["none", "nan", "nao identificado"]:
        return default
    return str(val)

def safe_delete_all():
    sb = get_supabase()
    status = st.empty()
    try:
        res = sb.table("leads").select("id", count="exact").limit(1).execute()
        total = res.count or 0
        if total == 0: return
        eliminados = 0
        while True:
            batch = sb.table("leads").select("id").limit(400).execute()
            ids = [x['id'] for x in batch.data]
            if not ids: break
            sb.table("leads").delete().in_("id", ids).execute()
            eliminados += len(ids)
            status.info(f"🧬 Limpando base: {eliminados}/{total}")
        st.rerun()
    except Exception as e: st.error(f"Erro: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 3. SIDEBAR (CONTROLES)
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("💎 Talent Control")
    busca = st.text_input("🔍 O que você procura?", placeholder="Digite um cargo ou nome...")
    
    st.divider()
    f_area = st.selectbox("Área", ["Todas", "Tech/Dev", "Vendas", "RH", "Executivo"])
    f_sen = st.selectbox("Nível", ["Todas", "Senior", "Pleno", "Junior"])

    st.divider()
    with st.expander("🛠️ Admin Tools"):
        file = st.file_uploader("Subir base", type=['csv', 'xlsx'])
        if st.button("LIMPAR BANCO", type="primary", use_container_width=True):
            safe_delete_all()

# ══════════════════════════════════════════════════════════════════════════════
# 4. DASHBOARD PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
sb = get_supabase()
# Pegamos o total para as métricas sem imprimir código
res_m = sb.table("leads").select("id", count="exact").limit(1).execute()
total_base = res_m.count or 0

st.title("Buscador de Talentos")
m1, m2, m3 = st.columns(3)
m1.metric("Leads Totais", f"{total_base:,}")
m2.metric("Qualidade", "98%", "Premium")
m3.metric("Status", "Conectado", delta="Live")
st.divider()

if not busca:
    st.markdown("""
    <div class="empty-state">
        <h3>🔍 Engine Pronta</h3>
        <p>Aguardando termo de busca para filtrar os melhores candidatos.</p>
    </div>
    """, unsafe_allow_html=True)
else:
    query = sb.table("leads").select("*")
    query = query.or_(f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%")
    if f_area != "Todas": query = query.eq("area_identificada", f_area)
    if f_sen != "Todas": query = query.eq("senioridade_normalizada", f_sen)
    
    results = query.order("id", desc=True).limit(40).execute().data

    if not results:
        st.warning(f"Nenhum resultado para '{busca}'.")
    else:
        # Título e Exportação
        c_tit, c_exp = st.columns([3, 1])
        c_tit.subheader(f"Exibindo talentos para: {busca}")
        with c_exp:
            towrite = io.BytesIO()
            pd.DataFrame(results).to_excel(towrite, index=False)
            st.download_button("📥 Baixar Lista", towrite.getvalue(), "leads.xlsx", use_container_width=True)

        for r in results:
            nome = clean_val(r.get('name'), "Candidato")
            cargo = clean_val(r.get('cargo'), "N/I")
            empresa = clean_val(r.get('company_name'), "Privada")
            salario = float(r.get('salario_estimado') or 0)
            url_li = r.get('linkedin_url')

            # CARD RENDER
            st.markdown(f"""
            <div class="lead-card">
                <div style="display: flex; justify-content: space-between;">
                    <div>
                        <div style="font-size: 1.3rem; font-weight: 800; color: #1E293B;">{nome}</div>
                        <div style="color: #64748B;">{cargo} • <b style="color: #4F46E5;">{empresa}</b></div>
                    </div>
                    <div class="salary-label">{f"R$ {salario:,.2f}" if salario > 0 else "Sob Consulta"}</div>
                </div>
                <div style="margin-top: 15px;">
                    {f'<span class="tag tag-blue">📁 {r.get("area_identificada")}</span>' if clean_val(r.get("area_identificada")) else ''}
                    {f'<span class="tag tag-amber">⚡ {r.get("senioridade_normalizada")}</span>' if clean_val(r.get("senioridade_normalizada")) else ''}
                    <span class="tag tag-slate">📍 {clean_val(r.get("cidade"), "Brasil")}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # O VOLTA DO "VAI PRO LINKEDIN"
            if url_li:
                st.link_button(f"🔥 Abrir perfil de {nome.split()[0]}", url_li, use_container_width=True)
                st.markdown("<br>", unsafe_allow_html=True) # Espaçamento entre cards
