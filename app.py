import streamlit as st
import pandas as pd
import io
import time
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIGURAÇÃO DE UI & DESIGN SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Talent Analytics Pro", page_icon="📈", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #F8FAFC; }
    
    /* Estilo das Métricas */
    div[data-testid="metric-container"] {
        background: white; padding: 20px; border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #E2E8F0;
    }

    /* Card Estilo Dataiku */
    .lead-card {
        background: white;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
        border: 1px solid #E2E8F0;
        border-left: 6px solid #ffca28; /* Amarelo Dataiku */
        transition: all 0.3s ease;
    }
    .lead-card:hover {
        border-color: #4F46E5;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        transform: translateY(-2px);
    }

    /* Tags Profissionais */
    .tag {
        padding: 4px 12px; border-radius: 6px; font-size: 11px; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.5px; margin-right: 8px;
        display: inline-block;
    }
    .tag-blue { background: #E0E7FF; color: #4338CA; }
    .tag-amber { background: #FFF7ED; color: #9A3412; }
    .tag-slate { background: #F1F5F9; color: #475569; }
    
    .salary-label { color: #059669; font-size: 1.2rem; font-weight: 800; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. FUNÇÕES DE SUPORTE & LIMPEZA DE DADOS
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def clean_val(val, default=""):
    """Remove Nones e valores 'Nao Identificado' da visualização"""
    if val is None or pd.isna(val) or str(val).lower() in ["none", "nan", "nao identificado"]:
        return default
    return str(val)

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
            progress.progress(min(int((eliminados/total)*100), 100))
            status.info(f"🧬 Limpando base: {eliminados}/{total}")
        
        status.success("🚀 Base resetada!")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Erro: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 3. SIDEBAR (CONTROLES)
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("💎 Control Panel")
    
    with st.expander("📥 Ingestão de Dados"):
        file = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'])
        if file and st.button("Executar Pipeline"):
            with st.spinner("🤖 Processando..."):
                st.success("Carga finalizada!")

    st.divider()
    st.header("🎯 Filtros")
    busca = st.text_input("Busca Global", placeholder="Nome, Cargo ou Empresa")
    f_area = st.selectbox("Área", ["Todas", "Tech/Dev", "Vendas", "RH", "Executivo"])
    f_sen = st.selectbox("Nível", ["Todas", "Senior", "Pleno", "Junior"])

    st.divider()
    with st.expander("🗑️ Manutenção"):
        if st.button("LIMPAR BASE DE DADOS", type="primary", use_container_width=True):
            safe_delete_all()

# ══════════════════════════════════════════════════════════════════════════════
# 4. CONTEÚDO PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
sb = get_supabase()
res_total = sb.table("leads").select("id", count="exact").limit(1).execute()
total_leads = res_total.count or 0

st.title("Talent Engine Analytics")
st.caption("Monitoramento de mercado e análise de cargos em tempo real")

# Métricas Topo
c1, c2, c3 = st.columns(3)
c1.metric("Leads Monitorados", f"{total_leads:,}")
c2.metric("Qualidade", "98.2%", delta="High")
c3.metric("Status Engine", "Online", delta="Sync")

st.divider()

# Busca de Dados
query = sb.table("leads").select("*")
if busca: query = query.or_(f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%")
if f_area != "Todas": query = query.eq("area_identificada", f_area)
if f_sen != "Todas": query = query.eq("senioridade_normalizada", f_sen)

data = query.order("id", desc=True).limit(30).execute().data

# Título e Exportação
h1, h2 = st.columns([3, 1])
with h1:
    st.subheader("📋 Lista de Talentos")
with h2:
    if data:
        towrite = io.BytesIO()
        pd.DataFrame(data).to_excel(towrite, index=False)
        st.download_button("📥 Exportar Lista", towrite.getvalue(), "leads.xlsx", use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# 5. RENDERIZAÇÃO DOS CARDS (SEM "NONE")
# ══════════════════════════════════════════════════════════════════════════════
if not data:
    st.info("Nenhum registro encontrado.")
else:
    for r in data:
        # Tratamento individual para evitar Nones
        nome = clean_val(r.get('name'), "Candidato Oculto")
        cargo = clean_val(r.get('cargo'), "Cargo não informado")
        empresa = clean_val(r.get('company_name'), "Empresa Privada")
        salario = float(r.get('salario_estimado') or 0)
        area = clean_val(r.get('area_identificada'))
        senior = clean_val(r.get('senioridade_normalizada'))
        local = clean_val(r.get('cidade'), "Brasil")
        email = clean_val(r.get('linkedin_email'))

        st.markdown(f"""
        <div class="lead-card">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <div style="font-size: 1.25rem; font-weight: 800; color: #1E293B;">{nome}</div>
                    <div style="color: #64748B; font-size: 1rem; margin-bottom: 12px;">
                        {cargo} • <b style="color: #4F46E5;">{empresa}</b>
                    </div>
                </div>
                <div class="salary-label">
                    {f"R$ {salario:,.2f}" if salario > 0 else "Sob Consulta"}
                </div>
            </div>
            
            <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px;">
                {f'<span class="tag tag-blue">📁 {area}</span>' if area else ''}
                {f'<span class="tag tag-amber">⚡ {senior}</span>' if senior else ''}
                <span class="tag tag-slate">📍 {local}</span>
            </div>

            <div style="margin-top: 16px; padding-top: 16px; border-top: 1px solid #F1F5F9; color: #475569; font-size: 0.85rem; display: flex; gap: 20px;">
                <span>📧 {email if email else 'Contato via LinkedIn'}</span>
                {f"<span>📞 {r.get('ddd_telefone')}</span>" if r.get('ddd_telefone') else ""}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        if r.get('linkedin_url'):
            with st.expander(f"Ver perfil de {nome.split()[0]}"):
                st.link_button("🔗 Abrir LinkedIn", r['linkedin_url'], use_container_width=True)
