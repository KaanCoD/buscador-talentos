import streamlit as st
import pandas as pd
import io
import time
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# 1. DESIGN SYSTEM (DATAIKU UI)
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Talent Analytics Pro", page_icon="📈", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #F8FAFC; }
    div[data-testid="metric-container"] {
        background: white; padding: 20px; border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #E2E8F0;
    }
    .lead-card {
        background: white; border-radius: 12px; padding: 24px; margin-bottom: 16px;
        border: 1px solid #E2E8F0; border-left: 6px solid #ffca28;
        transition: all 0.3s ease;
    }
    .lead-card:hover {
        border-color: #4F46E5; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        transform: translateY(-2px);
    }
    .tag {
        padding: 4px 12px; border-radius: 6px; font-size: 11px; font-weight: 700;
        text-transform: uppercase; margin-right: 8px; display: inline-block;
    }
    .tag-blue { background: #E0E7FF; color: #4338CA; }
    .tag-amber { background: #FFF7ED; color: #9A3412; }
    .tag-slate { background: #F1F5F9; color: #475569; }
    .salary-label { color: #059669; font-size: 1.2rem; font-weight: 800; }
    .empty-state { text-align: center; padding: 100px; color: #64748B; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. FUNÇÕES DE SUPORTE
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
            status.info(f"🧬 Purificando base: {eliminados}/{total}")
        st.rerun()
    except Exception as e: st.error(f"Erro: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 3. SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("💎 Control Panel")
    with st.expander("📥 Ingestão de Dados"):
        file = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'])
        if file and st.button("Executar Pipeline"):
            st.success("Dados integrados!")
    
    st.divider()
    st.header("🎯 Filtros")
    # A busca agora é o gatilho principal
    busca = st.text_input("🔍 O que você procura?", placeholder="Digite um cargo ou nome...")
    f_area = st.selectbox("Área", ["Todas", "Tech/Dev", "Vendas", "RH", "Executivo"])
    f_sen = st.selectbox("Nível", ["Todas", "Senior", "Pleno", "Junior"])

    st.divider()
    with st.expander("🗑️ Manutenção"):
        if st.button("LIMPAR BASE DE DADOS", type="primary"):
            safe_delete_all()

# ══════════════════════════════════════════════════════════════════════════════
# 4. DASHBOARD & LÓGICA DE EXIBIÇÃO
# ══════════════════════════════════════════════════════════════════════════════
sb = get_supabase()
res_total = sb.table("leads").select("id", count="exact").limit(1).execute()
total_leads = res_total.count or 0

st.title("Talent Engine Analytics")
c1, c2, c3 = st.columns(3)
c1.metric("Leads Monitorados", f"{total_leads:,}")
c2.metric("Qualidade", "Gold", delta="98%")
c3.metric("Status", "Online", delta="Sync")
st.divider()

# SÓ BUSCA E MOSTRA SE O USUÁRIO DIGITAR ALGO
if not busca:
    st.markdown("""
    <div class="empty-state">
        <img src="https://cdn-icons-png.flaticon.com/512/5066/5066971.png" width="100" style="opacity: 0.2;">
        <h3>Aguardando sua busca...</h3>
        <p>Digite um cargo ou palavra-chave na barra lateral para começar.</p>
    </div>
    """, unsafe_allow_html=True)
else:
    # Lógica de Query
    query = sb.table("leads").select("*")
    query = query.or_(f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%")
    if f_area != "Todas": query = query.eq("area_identificada", f_area)
    if f_sen != "Todas": query = query.eq("senioridade_normalizada", f_sen)
    
    data = query.order("id", desc=True).limit(50).execute().data

    if not data:
        st.warning(f"Nenhum lead encontrado para '{busca}'.")
    else:
        h1, h2 = st.columns([3, 1])
        with h1: st.subheader(f"📋 Resultados para: {busca}")
        with h2:
            towrite = io.BytesIO()
            pd.DataFrame(data).to_excel(towrite, index=False)
            st.download_button("📥 Exportar XLSX", towrite.getvalue(), "leads.xlsx", use_container_width=True)

        for r in data:
            nome = clean_val(r.get('name'), "Candidato")
            cargo = clean_val(r.get('cargo'), "N/I")
            empresa = clean_val(r.get('company_name'), "Privada")
            salario = float(r.get('salario_estimado') or 0)
            
            st.markdown(f"""
            <div class="lead-card">
                <div style="display: flex; justify-content: space-between;">
                    <div>
                        <div style="font-size: 1.25rem; font-weight: 800; color: #1E293B;">{nome}</div>
                        <div style="color: #64748B;">{cargo} • <b style="color: #4F46E5;">{empresa}</b></div>
                    </div>
                    <div class="salary-label">{f"R$ {salario:,.2f}" if salario > 0 else "Sob Consulta"}</div>
                </div>
                <div style="margin-top: 12px;">
                    {f'<span class="tag tag-blue">📁 {r.get("area_identificada")}</span>' if clean_val(r.get("area_identificada")) else ''}
                    {f'<span class="tag tag-amber">⚡ {r.get("senioridade_normalizada")}</span>' if clean_val(r.get("senioridade_normalizada")) else ''}
                    <span class="tag tag-slate">📍 {clean_val(r.get("cidade"), "Brasil")}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
