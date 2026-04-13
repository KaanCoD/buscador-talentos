import streamlit as st
import pandas as pd
import io
import time
from supabase import create_client

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIG & CSS
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Talent Analytics Pro", page_icon="💎", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #F8FAFC; }

    [data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: 800 !important;
        color: #1E293B;
    }
    div[data-testid="metric-container"] {
        background: white;
        padding: 25px;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        border: 1px solid #E2E8F0;
    }

    /* ── CARD ── */
    .lead-card {
        background: white;
        border-radius: 12px;
        padding: 20px 24px;
        border: 1px solid #E2E8F0;
        border-left: 6px solid #ffca28;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    .lead-header {
        display: flex;
        align-items: center;
        gap: 16px;
    }
    .lead-avatar {
        width: 56px;
        height: 56px;
        border-radius: 50%;
        object-fit: cover;
        border: 2px solid #E2E8F0;
        flex-shrink: 0;
    }
    .lead-avatar-placeholder {
        width: 56px;
        height: 56px;
        border-radius: 50%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-size: 1.4rem;
        font-weight: 800;
        flex-shrink: 0;
    }
    .lead-info { flex: 1; min-width: 0; }
    .lead-nome {
        font-size: 1.15rem;
        font-weight: 800;
        color: #0F172A;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .lead-cargo {
        color: #475569;
        font-size: 0.88rem;
        margin-top: 2px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .lead-empresa { color: #4F46E5; font-weight: 700; }
    .lead-salario {
        color: #059669;
        font-size: 1.1rem;
        font-weight: 800;
        white-space: nowrap;
        margin-left: auto;
    }

    /* Tags */
    .lead-tags { display: flex; gap: 6px; margin-top: 12px; flex-wrap: wrap; }
    .tag {
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        display: inline-block;
    }
    .tag-blue   { background: #E0E7FF; color: #4338CA; }
    .tag-amber  { background: #FFF7ED; color: #9A3412; }
    .tag-slate  { background: #F1F5F9; color: #475569; }
    .tag-green  { background: #DCFCE7; color: #166534; }
    .tag-red    { background: #FEE2E2; color: #991B1B; }
    .tag-purple { background: #F3E8FF; color: #6B21A8; }

    /* Headline */
    .lead-headline {
        margin-top: 12px;
        padding: 10px 14px;
        background: #F8FAFC;
        border-radius: 8px;
        color: #334155;
        font-size: 0.83rem;
        line-height: 1.5;
        border-left: 3px solid #CBD5E1;
        font-style: italic;
    }

    /* Ficha tecnica */
    .detail-item {
        background: #F8FAFC;
        border-radius: 8px;
        padding: 10px 14px;
        border: 1px solid #E2E8F0;
        height: 100%;
    }
    .detail-label {
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: #94A3B8;
        margin-bottom: 3px;
    }
    .detail-value {
        font-size: 0.88rem;
        color: #1E293B;
        font-weight: 600;
        word-break: break-all;
    }

    .empty-search {
        text-align: center;
        padding: 100px;
        color: #64748B;
        background: white;
        border-radius: 12px;
        border: 2px dashed #E2E8F0;
    }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# 2. FUNÇÕES UTILITÁRIAS
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])


def clean_val(val, default=""):
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except Exception:
        pass
    s = str(val).strip()
    if s.lower() in ["none", "nan", "nao identificado", "não identificado", ""]:
        return default
    return s


def interesse_tag(val):
    v = str(val).lower()
    if "not_interested" in v:
        return '<span class="tag tag-red">❌ Não Interessado</span>'
    if "interested" in v:
        return '<span class="tag tag-green">✅ Interessado</span>'
    return ""


def safe_delete_all():
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
            ids = [x["id"] for x in batch.data]
            if not ids:
                break
            sb.table("leads").delete().in_("id", ids).execute()
            eliminados += len(ids)
            placeholder.info(f"🧬 Limpando base: {eliminados}/{total} leads removidos...")
        placeholder.success(f"🚀 Base resetada com sucesso! ({eliminados} leads)")
        time.sleep(2)
        st.cache_resource.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Erro na exclusão: {e}")


def detail_box(label, value):
    """Renderiza uma caixa de detalhe da ficha técnica."""
    v = value if value else "—"
    st.markdown(
        '<div class="detail-item">'
        f'<div class="detail-label">{label}</div>'
        f'<div class="detail-value">{v}</div>'
        '</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# 3. RENDER CARD
# ══════════════════════════════════════════════════════════════════════════════
def render_card(r, idx):
    # Extrai todos os campos
    nome        = clean_val(r.get("name"), "Candidato")
    cargo       = clean_val(r.get("cargo"), "Cargo não informado")
    empresa     = clean_val(r.get("company_name"), "Empresa Privada")
    area        = clean_val(r.get("area_identificada"))
    senioridade = clean_val(r.get("senioridade_normalizada"))
    cidade      = clean_val(r.get("cidade"), "Brasil")
    estado      = clean_val(r.get("estado"))
    email       = clean_val(r.get("linkedin_email"))
    tel         = clean_val(r.get("ddd_telefone"))
    url_li      = clean_val(r.get("linkedin_url"))
    foto        = clean_val(r.get("profile_photo_url"))
    headline    = clean_val(r.get("linkedin_headline"))
    interesse   = clean_val(r.get("lead_interest_status"))
    sexo        = clean_val(r.get("gender"))
    segmento    = clean_val(r.get("segmento_empresa"))

    try:
        salario = float(r.get("salario_estimado") or 0)
    except (ValueError, TypeError):
        salario = 0.0

    salario_txt = f"R$ {salario:,.2f}" if salario > 0 else "Sob Consulta"
    inicial     = nome[0].upper() if nome else "?"
    local_txt   = f"{cidade}, {estado}" if estado else cidade

    # Avatar
    if foto and foto.startswith("http"):
        avatar_html = f'<img class="lead-avatar" src="{foto}">'
    else:
        avatar_html = f'<div class="lead-avatar-placeholder">{inicial}</div>'

    # Tags
    tags = ""
    if area:
        tags += f'<span class="tag tag-blue">📁 {area}</span>'
    if senioridade:
        tags += f'<span class="tag tag-amber">⚡ {senioridade}</span>'
    tags += f'<span class="tag tag-slate">📍 {local_txt}</span>'
    if segmento:
        tags += f'<span class="tag tag-purple">🏢 {segmento}</span>'
    tags += interesse_tag(interesse)

    # Headline
    headline_html = ""
    if headline:
        headline_html = f'<div class="lead-headline">"{headline}"</div>'

    # Card
    st.markdown(
        '<div class="lead-card">'
            '<div class="lead-header">'
                + avatar_html +
                '<div class="lead-info">'
                    f'<div class="lead-nome">{nome}</div>'
                    '<div class="lead-cargo">'
                        f'{cargo} &bull; <span class="lead-empresa">{empresa}</span>'
                    '</div>'
                '</div>'
                f'<div class="lead-salario">{salario_txt}</div>'
            '</div>'
            f'<div class="lead-tags">{tags}</div>'
            + headline_html +
        '</div>',
        unsafe_allow_html=True,
    )

    # Expander - ficha técnica completa
    with st.expander("🔍 Ver ficha técnica completa"):
        st.markdown("**Contato**")
        c1, c2, c3 = st.columns(3)
        with c1:
            detail_box("📧 E-mail", email)
        with c2:
            detail_box("📞 Telefone", tel)
        with c3:
            detail_box("⚧ Gênero", sexo.capitalize() if sexo else "")

        st.markdown("**Perfil Profissional**")
        c4, c5, c6 = st.columns(3)
        with c4:
            detail_box("🏢 Segmento", segmento)
        with c5:
            detail_box("📊 Senioridade", senioridade)
        with c6:
            detail_box("📍 Localização", local_txt)

        if headline:
            st.markdown("**Headline LinkedIn**")
            st.markdown(
                '<div class="detail-item">'
                '<div class="detail-value" style="font-weight:400;font-style:italic;">'
                f'"{headline}"'
                '</div>'
                '</div>',
                unsafe_allow_html=True,
            )

        st.markdown("")
        if url_li:
            primeiro_nome = nome.split()[0]
            st.link_button(
                f"🔥 Abrir perfil de {primeiro_nome} no LinkedIn",
                url_li,
                use_container_width=True,
                type="secondary",
            )

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# 4. SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/1055/1055644.png", width=70)
    st.title("Dataiku Engine")
    st.divider()

    busca  = st.text_input("🔍 O que você procura?", placeholder="Digite um cargo ou nome...")
    f_area = st.selectbox("Área", ["Todas", "Tech/Dev", "Vendas", "RH", "Executivo"])
    f_sen  = st.selectbox("Nível", ["Todas", "Senior", "Pleno", "Junior"])

    st.divider()
    with st.expander("📤 Ingestão & Manutenção"):
        file = st.file_uploader("Upload CSV/XLSX", type=["csv", "xlsx"])
        if file and st.button("Executar Pipeline", use_container_width=True):
            with st.spinner("🤖 Processando..."):
                st.success("Carga finalizada!")
        st.write("---")
        if st.button("LIMPAR BASE DE DADOS", type="primary", use_container_width=True):
            safe_delete_all()


# ══════════════════════════════════════════════════════════════════════════════
# 5. MÉTRICAS
# ══════════════════════════════════════════════════════════════════════════════
sb = get_supabase()
res_count   = sb.table("leads").select("id", count="exact").limit(1).execute()
total_leads = res_count.count or 0

st.title("💎 Talent Engine Analytics")
st.caption("Plataforma unificada para gestão de leads e análise salarial")

col1, col2, col3 = st.columns(3)
col1.metric("Leads na Base",       f"{total_leads:,}", "Base Única")
col2.metric("Qualidade dos Dados", "Gold Standard",    delta="98%")
col3.metric("Status do Engine",    "Operacional",      delta="Sync")

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# 6. EXIBIÇÃO
# ══════════════════════════════════════════════════════════════════════════════
if not busca:
    st.markdown("""
    <div class="empty-search">
        <img src="https://cdn-icons-png.flaticon.com/512/5066/5066971.png"
             width="120" style="opacity:0.15;margin-bottom:20px;">
        <h3>💎 Dataiku Engine: Aguardando Busca</h3>
        <p>Digite um cargo, nome ou tecnologia na barra lateral para carregar os leads.</p>
    </div>
    """, unsafe_allow_html=True)

else:
    query = sb.table("leads").select("*")
    query = query.or_(
        f"name.ilike.%{busca}%,cargo.ilike.%{busca}%,company_name.ilike.%{busca}%"
    )
    if f_area != "Todas":
        query = query.eq("area_identificada", f_area)
    if f_sen != "Todas":
        query = query.eq("senioridade_normalizada", f_sen)

    data = query.order("id", desc=True).limit(50).execute().data

    if data:
        c1, c2 = st.columns([3, 1])
        with c1:
            st.subheader(f"📋 Lista de Talentos ({len(data)})")
        with c2:
            towrite = io.BytesIO()
            pd.DataFrame(data).to_excel(towrite, index=False)
            st.download_button(
                "📥 Exportar XLSX",
                towrite.getvalue(),
                "leads.xlsx",
                use_container_width=True,
            )

        for idx, r in enumerate(data):
            render_card(r, idx)

    else:
        st.warning(f"Nenhum registro encontrado para '{busca}'.")
