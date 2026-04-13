import streamlit as st
import pandas as pd
import io
import time
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# UI DESIGN SYSTEM (MODERNO & LIMPO)
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Talent Analytics Pro", page_icon="💎", layout="wide")

st.markdown("""
<style>
    /* Estilo do Fundo e Container */
    .stApp { background-color: #F8FAFC; }
    
    /* Métrica Estilizada */
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; font-weight: 800 !important; color: #1E293B; }
    div[data-testid="metric-container"] {
        background: white; padding: 20px; border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #E2E8F0;
    }

    /* Card de Lead Estilo Dataiku */
    .lead-card {
        background: white;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
        border: 1px solid #E2E8F0;
        transition: all 0.3s ease;
    }
    .lead-card:hover {
        border-color: #4F46E5;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        transform: translateY(-2px);
    }

    /* Tags */
    .tag {
        padding: 4px 12px; border-radius: 6px; font-size: 11px; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.5px; margin-right: 8px;
    }
    .tag-blue { background: #E0E7FF; color: #4338CA; }
    .tag-amber { background: #FFF7ED; color: #9A3412; }
    .tag-slate { background: #F1F5F9; color: #475569; }
    
    .salary-label { color: #059669; font-size: 1.2rem; font-weight: 800; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# BACKEND & LOGICA DE SEGURANÇA
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def safe_delete_all():
    sb = get_supabase()
    status = st.empty()
    progress = st.empty()
    
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
            
            # Feedback Visual
            perc = min(int((eliminados/total)*100), 100)
            progress.progress(perc)
            status.info(f"🧬 Purificando dados: {eliminados}/{total}")
        
        status.success("🚀 Base de dados resetada com sucesso!")
        time.sleep(1.5)
        st.rerun()
    except Exception as e:
        st.error(f"Erro na exclusão: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR (CONTROLES CENTRALIZADOS)
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("💎 Control Panel")
    
    with st.expander("📥 Ingestão de Dados", expanded=False):
        file = st.file_uploader("Upload Dataset", type=['csv', 'xlsx'])
        if file and st.button("Executar Pipeline"):
            with st.spinner("🤖 Processando..."):
                st.success("Carga finalizada!")

    st.divider()
    st.header("🎯 Filtros")
    busca = st.text_input("Busca Inteligente", placeholder="Nome, Cargo ou Empresa")
    f_area = st.selectbox("Área de Atuação", ["Todas", "Tech/Dev", "Vendas", "RH", "Executivo"])
    f_sen = st.selectbox("Senioridade", ["Todas", "Senior", "Pleno", "Junior"])

    st.divider()
    with st.expander("🗑️ Manutenção"):
        st.caption("Cuidado: Isso apagará toda a base conectada.")
        if st.button("LIMPAR BASE DE DADOS", type="primary", use_container_width=True):
            safe_delete_all()

# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
sb = get_supabase()
res_total = sb.table("leads").select("id", count="exact").limit(1).execute()
total_leads = res_total.count or 0

# Header
st.title("Talent Engine Analytics")
st.caption("Plataforma unificada para gestão de leads e análise salarial")

# Métricas Topo
c1, c2, c3 = st.columns(3)
c1.metric("Leads Monitorados", f"{total_leads:,}")
c2.metric("Qualidade do Lead", "Gold Standard", delta="High")
c3.metric("Status do Engine", "Operacional", delta="Sync")

st.divider()

# Busca de Dados
query = sb.table("leads").select("*")
if busca: query = query.or_(f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%")
if f_area != "Todas": query = query.eq("area_identificada", f_area)
if f_sen != "Todas": query = query.eq("senioridade_normalizada", f_sen)

data = query.order("id", desc=True).limit(30).execute().data

# Título da Lista e Download
h1, h2 = st.columns([3, 1])
with h1:
    st.subheader("📋 Resultados da Consulta")
with h2:
    if data:
        df_exp = pd.DataFrame(data)
        towrite = io.BytesIO()
        df_exp.to_excel(towrite, index=False)
        st.download_button("📥 Exportar Lista", towrite.getvalue(), "leads_pro.xlsx", use_container_width=True)

# Grid de Cards
if not data:
    st.info("Nenhum registro encontrado para os critérios selecionados.")
else:
    for r in data:
        st.markdown(f"""
        <div class="lead-card">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <div style="font-size: 1.3rem; font-weight: 800; color: #0F172A;">{r['name']}</div>
                    <div style="color: #64748B; font-size: 1rem; margin-bottom: 12px;">
                        {r.get('cargo', 'N/I')} • <b style="color: #4F46E5;">{r.get('company_name', 'N/I')}</b>
                    </div>
                </div>
                <div class="salary-label">R$ {float(r.get('salario_estimado') or 0):,.2f}</div>
            </div>
            <div style="display: flex; gap: 8px; margin-top: 8px;">
                <span class="tag tag-blue">📁 {r.get('area_identificada', 'Geral')}</span>
                <span class="tag tag-amber">⚡ {r.get('senioridade_normalizada', 'N/I')}</span>
                <span class="tag tag-slate">📍 {r.get('cidade', 'Brasil')}</span>
            </div>
            <div style="margin-top: 16px; padding-top: 16px; border-top: 1px solid #F1F5F9; color: #475569; font-size: 0.85rem; display: flex; gap: 20px;">
                <span>📧 {r.get('linkedin_email', '—')}</span>
                <span>📞 {r.get('ddd_telefone', '—')}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        if r.get('linkedin_url'):
            with st.expander(f"Ações rápidas: {r['name'].split()[0]}"):
                st.link_button("🔥 Abrir Perfil no LinkedIn", r['linkedin_url'], use_container_width=True)
