import pandas as pd
import numpy as np
import plotly.express as px
import openpyxl

from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pyodbc

# =========================================================
# 0) CONFIG / CONSTANTES
# =========================================================
BOOTSTRAP_ICONS = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css"
APP_TITLE = "Aprovações - Compras (Protheus)"
REFRESH_MS = 2 * 60 * 1000  # 2 min

PAGE_STYLE = {"padding": "12px", "backgroundColor": "#a5aeb8", "minHeight": "100vh"}

HEADER_STYLE = {
    "backgroundColor": "#E9ECEF",
    "color": "#C02626",
    "fontWeight": "600",
    "textAlign": "center",
    "padding": "6px 10px",
    "borderBottom": "1px solid #CED4DA",
}

CARD_STYLE = {"border": "1px solid #CED4DA", "borderRadius": "10px", "overflow": "hidden"}

TABLE_HEADER_STYLE = {
    "backgroundColor": "#7a7a7a",
    "color": "white",
    "fontWeight": "bold",
    "textAlign": "center",
    "border": "1px solid #1f4d57",
}

TABLE_CELL_STYLE = {
    "fontFamily": "Arial",
    "fontSize": "13px",
    "padding": "8px",
    "border": "1px solid #1f4d57",
    "whiteSpace": "normal",
    "height": "auto",
}

# =========================================================
# 1) CONEXÃO / QUERIES
# =========================================================
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.0.244,1433;"
    "DATABASE=P12_PRODUCAO;"
    "UID=consulta;"
    "PWD=G@l@t@s2:20;"
)

sql_query_main = """
SELECT DISTINCT
    SAL.AL_COD AS COD_GRUPO_APROVADOR,
    C7.C7_NUM AS NUM_PEDIDO,
    C7.C7_NUMSC AS NUM_SOLICITACAO,
    C7.C7_EMISSAO AS DT_EMISSAO,
    E2.E2_VENCTO AS DATA_VENCIMENTO,
    C7.C7_CC AS CENTRO_CUSTO,
    CC.CTT_DESC01 AS DESCR_CC,
    C7.C7_DESCRI,
    C7.C7_FORNECE AS COD_FORNECEDOR,
    C7.C7_LOJA,
    C7.C7_CONTRA AS CONTRATO,
    C7.C7_TOTAL AS VALOR_TOTAL,
    C7.C7_MEDICAO,
    D1.D1_DOC AS NUM_DOCUMENTO,
    D1.D1_TES AS TIPO_DOCUMENTO,
    FORN.A2_NOME AS NOME_FORNECEDOR,
    SAL.AL_NIVEL AS NIVEL,
    SAL.AL_APROV AS COD_APROVADOR,
    USR.AK_NOME AS NOME_APROVADOR,
    CR.CR_DATALIB,
    CASE
        WHEN NULLIF(LTRIM(RTRIM(CR.CR_DATALIB)), '') IS NOT NULL THEN 'APROVADO'
        ELSE 'PENDENTE'
    END AS STATUS_APROVACAO,
    SU.USR_NOME AS NOME_REQUISITANTE
FROM SAK010 USR
LEFT JOIN SAL010 SAL
    ON SAL.AL_APROV = USR.AK_COD
   AND SAL.AL_FILIAL = USR.AK_FILIAL
   AND SAL.D_E_L_E_T_ = ''
LEFT JOIN SC7010 C7
    ON C7.C7_APROV = SAL.AL_COD
   AND C7.C7_FILIAL = SAL.AL_FILIAL
   AND C7.D_E_L_E_T_ = ''
LEFT JOIN SCR010 CR
    ON USR.AK_COD = CR.CR_APROV
   AND USR.AK_FILIAL = CR.CR_FILIAL
   AND C7.C7_NUM = CR.CR_NUM
   AND CR.D_E_L_E_T_ = ''
LEFT JOIN CTT010 CC
    ON CC.CTT_CUSTO = C7.C7_CC
   AND CC.D_E_L_E_T_ = ''
LEFT JOIN SA2010 FORN
    ON FORN.A2_COD = C7.C7_FORNECE
   AND FORN.A2_LOJA = C7.C7_LOJA
   AND FORN.D_E_L_E_T_ = '' 
LEFT JOIN SYS_USR SU
    ON C7.C7_USER = SU.USR_ID
   AND SU.D_E_L_E_T_ = ''
LEFT JOIN SD1010 D1
    ON D1.D1_PEDIDO = C7.C7_NUM
   AND D1.D1_ITEMPC = C7.C7_ITEM
   AND D1.D1_FORNECE = C7.C7_FORNECE
   AND D1.D1_LOJA = C7.C7_LOJA
   AND D1.D_E_L_E_T_ = ''
LEFT JOIN SE2010 E2
    ON E2.E2_NUM = D1.D1_DOC
   AND E2.E2_FILIAL = D1.D1_FILIAL
   AND E2.E2_FORNECE = C7.C7_FORNECE
   AND E2.E2_LOJA = C7.C7_LOJA
   AND E2.D_E_L_E_T_ = ''
LEFT JOIN SF4010 F4
        ON F4.F4_CODIGO = D1.D1_TES
        AND F4.D_E_L_E_T_ = ''
WHERE
    USR.D_E_L_E_T_ = ''
    AND SAL.AL_MSBLQL = '2'
    AND C7.C7_EMISSAO >= DATEADD(MONTH, -6, GETDATE())
    AND A2_COD <> '001128' -- COMPASA
ORDER BY SAL.AL_NIVEL ASC;
"""

# SD1 -> SE2 (independente de pedido)
# ATENÇÃO: confirme se o seu campo é D1_TOTAL mesmo (em alguns Protheus pode ser D1_VALOR/D1_VTOTAL)
sql_query_sd1 = """


SELECT
    FORN.A2_NOME AS NOME_FORNECEDOR,
    D1.D1_DOC    AS NUM_DOCUMENTO,
    D1.D1_TES    AS TIPO_TESOURARIA,
    D1.D1_EMISSAO AS DT_EMISSAO,
    E2.E2_VENCTO AS DATA_VENCIMENTO,
    E2.E2_SALDO  AS VALOR_SALDO,
    (D1_TOTAL - D1_VALPIS - D1_VALCOF - D1_VALIRR - D1_DESC + D1_DESPESA + D1_VALFRE)  AS VALOR_FINANCEIRO,
    D1.D1_TOTAL  AS VALOR_SD1
FROM SD1010 D1
LEFT JOIN SF4010 F4
        ON F4.F4_CODIGO = D1.D1_TES
        AND F4.D_E_L_E_T_ = ''
LEFT JOIN SE2010 E2
    ON E2.E2_NUM     = D1.D1_DOC
   AND E2.E2_FILIAL  = D1.D1_FILIAL
   AND E2.E2_FORNECE = D1.D1_FORNECE
   AND E2.E2_LOJA    = D1.D1_LOJA
   AND E2.D_E_L_E_T_ = ''
   AND E2.E2_VENCTO >= CONVERT(VARCHAR(8), DATEADD(MONTH, -13, GETDATE()), 112)
LEFT JOIN SA2010 FORN
    ON FORN.A2_COD  = D1.D1_FORNECE
   AND FORN.A2_LOJA = D1.D1_LOJA
   AND FORN.D_E_L_E_T_ = ''
WHERE
    D1.D_E_L_E_T_ = '' AND D1_TES <> '' AND F4.F4_DUPLIC = 'S' AND A2_COD <> '001128' and D1.D1_EMISSAO >= DATEADD(MONTH, -13, GETDATE());
"""

# =========================================================
# 2) HELPERS
# =========================================================
def opts_from_series(s: pd.Series):
    if s is None:
        return []
    vals = (
        s.dropna()
        .astype(str)
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
        .dropna()
        .unique()
        .tolist()
    )
    return [{"label": v, "value": v} for v in sorted(vals)]

def kpi_card(titulo, valor, sub="", icon="bi bi-bar-chart"):
    return dbc.Card(
        dbc.CardBody(
            dbc.Row(
                [
                    dbc.Col(html.I(className=icon, style={"fontSize": "34px", "color": "#0d6efd"}), width="auto"),
                    dbc.Col(
                        [
                            html.Div(titulo, className="text-muted", style={"fontSize": "12px"}),
                            html.Div(valor, style={"fontSize": "28px", "fontWeight": "700", "lineHeight": "1"}),
                            html.Div(sub, className="text-muted", style={"fontSize": "12px"}) if sub else None,
                        ],
                        style={"minWidth": 0},
                    ),
                ],
                align="center",
                className="g-2",
            ),
            style={"height": "110px", "display": "flex", "alignItems": "center", "justifyContent": "center"},
        ),
        className="shadow-sm w-100",
    )

def card_com_header(titulo, graph_id):
    return dbc.Card(
        [
            dbc.CardHeader(titulo, style=HEADER_STYLE),
            dbc.CardBody(dcc.Graph(id=graph_id, config={"displayModeBar": False}), style={"padding": "6px"}),
        ],
        style=CARD_STYLE,
        className="shadow-sm w-100",
    )

def _to_int_or_nan(x):
    try:
        return int(str(x).strip())
    except Exception:
        return np.nan

def _norm_null_series(s: pd.Series, idx):
    if s is None:
        return pd.Series([np.nan] * len(idx), index=idx)
    return (
        s.astype(str)
        .str.strip()
        .replace({"": np.nan, "None": np.nan, "nan": np.nan})
    )

def format_rs(v):
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def br_int(n):
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return "0"

def make_badge(text, color="secondary"):
    return dbc.Badge(text, color=color, className="ms-2", pill=True)

# =========================================================
# 3) DATA LAYER
# =========================================================
def get_data_main() -> pd.DataFrame:
    with pyodbc.connect(CONN_STR) as conn:
        return pd.read_sql(sql_query_main, conn)

def get_data_sd1() -> pd.DataFrame:
    with pyodbc.connect(CONN_STR) as conn:
        return pd.read_sql(sql_query_sd1, conn)

def preparar_campos_main(dff: pd.DataFrame) -> pd.DataFrame:
    if dff.empty:
        return dff

    if "DT_EMISSAO" in dff.columns:
        dff["DT_EMISSAO_DT"] = pd.to_datetime(
            dff["DT_EMISSAO"].astype(str),
            format="%Y%m%d",
            errors="coerce"
        )
        dff["MES_EMISSAO"] = dff["DT_EMISSAO_DT"].dt.to_period("M").astype(str).str.replace("-", "/")
        dff["DT_EMISSAO"] = dff["DT_EMISSAO_DT"].dt.strftime("%d/%m/%Y")

    if "DATA_VENCIMENTO" in dff.columns:
        dff["DATA_VENCIMENTO_DT"] = pd.to_datetime(dff["DATA_VENCIMENTO"].astype(str), format="%Y%m%d", errors="coerce")
        dff["MES_VENCIMENTO"] = dff["DATA_VENCIMENTO_DT"].dt.to_period("M").astype(str).str.replace("-", "/")
        dff["DATA_VENCIMENTO"] = dff["DATA_VENCIMENTO_DT"].dt.strftime("%d/%m/%Y")

    if "STATUS_APROVACAO" in dff.columns:
        dff["STATUS_APROVACAO"] = dff["STATUS_APROVACAO"].astype(str).str.strip().str.upper()

    for c in [
        "NUM_PEDIDO", "NOME_FORNECEDOR", "CENTRO_CUSTO", "NOME_APROVADOR",
        "DESCR_CC", "NIVEL", "NOME_REQUISITANTE", "CONTRATO", "COD_FORNECEDOR"
    ]:
        if c in dff.columns:
            dff[c] = dff[c].astype(str).str.strip()

    if "VALOR_TOTAL" in dff.columns:
        dff["VALOR_TOTAL"] = pd.to_numeric(dff["VALOR_TOTAL"], errors="coerce").fillna(0)

    return dff

def preparar_campos_sd1(dff: pd.DataFrame) -> pd.DataFrame:
    if dff.empty:
        return dff

    if "DATA_VENCIMENTO" in dff.columns:
        dff["DATA_VENCIMENTO_DT"] = pd.to_datetime(dff["DATA_VENCIMENTO"].astype(str), format="%Y%m%d", errors="coerce")
        dff["MES_VENCIMENTO"] = dff["DATA_VENCIMENTO_DT"].dt.to_period("M").astype(str)  # YYYY-MM
        dff["DATA_VENCIMENTO"] = dff["DATA_VENCIMENTO_DT"].dt.strftime("%d/%m/%Y")
        
    if "NUM_DOCUMENTO" in dff.columns:
        dff["NUM_DOCUMENTO"] = dff["NUM_DOCUMENTO"].astype(str).str.strip()

    if "NOME_FORNECEDOR" in dff.columns:
        dff["NOME_FORNECEDOR"] = dff["NOME_FORNECEDOR"].astype(str).str.strip()

    for c in ["VALOR_SD1", "VALOR_SALDO", "VALOR_FINANCEIRO"]:
        if c in dff.columns:
            dff[c] = pd.to_numeric(dff[c], errors="coerce").fillna(0)

    if "DATA_VENCIMENTO_DT" in dff.columns:
        dff["DATA_VENCIMENTO_DT"] = pd.to_datetime(dff["DATA_VENCIMENTO_DT"], errors="coerce")
    return dff

# =========================================================
# 4) FIGURES
# =========================================================
def build_timeline_figure(dff: pd.DataFrame, max_pedidos: int = 40, max_nomes_por_nivel: int = 3):
    if dff.empty:
        return px.scatter(pd.DataFrame({"NIVEL_LABEL": [], "NUM_PEDIDO": [], "STATUS_FLUXO": []}),
                          x="NIVEL_LABEL", y="NUM_PEDIDO", color="STATUS_FLUXO")

    df = dff.copy()
    df["NIVEL_NUM"] = df["NIVEL"].apply(_to_int_or_nan)

    # nomes por nível
    tmp_names = df.copy()
    tmp_names["NOME_APROVADOR"] = (
        tmp_names.get("NOME_APROVADOR", "")
        .astype(str).str.strip()
        .replace({"": np.nan, "None": np.nan, "nan": np.nan})
    )

    def pick_names(s: pd.Series) -> str:
        s = s.dropna().astype(str).str.strip()
        if s.empty:
            return ""
        nomes = s.value_counts().head(max_nomes_por_nivel).index.tolist()
        return "<br>".join(nomes)

    map_aprov_nivel = (
        tmp_names.dropna(subset=["NIVEL_NUM"])
        .groupby("NIVEL_NUM")["NOME_APROVADOR"]
        .apply(pick_names)
        .to_dict()
    )

    def mk_label(n):
        if pd.isna(n):
            return "N/I"
        n_int = int(n)
        nm = str(map_aprov_nivel.get(n_int, "")).strip()
        return f"{n_int} - {nm}" if nm else f"{n_int}"

    df["NIVEL_LABEL"] = df["NIVEL_NUM"].apply(mk_label)

    # limita pedidos por data
    if "DT_EMISSAO_DT" in df.columns:
        pedidos_top = (
            df.dropna(subset=["DT_EMISSAO_DT"])
            .groupby("NUM_PEDIDO", as_index=False)["DT_EMISSAO_DT"].max()
            .sort_values("DT_EMISSAO_DT", ascending=False)["NUM_PEDIDO"]
            .astype(str).head(max_pedidos).tolist()
        )
        df = df[df["NUM_PEDIDO"].astype(str).isin(pedidos_top)]


 # status do fluxo (4 cores)
    num_doc_ok = _norm_null_series(df.get("NUM_DOCUMENTO"), df.index)
    tipo_doc_ok = _norm_null_series(df.get("TIPO_DOCUMENTO"), df.index)

    if "STATUS_APROVACAO" not in df.columns:
        df["STATUS_APROVACAO"] = "PENDENTE"

    df["STATUS_FLUXO"] = df["STATUS_APROVACAO"].astype(str).str.strip().str.upper()
    df["STATUS_FLUXO"] = df["STATUS_FLUXO"].replace({"AGUARDANDO": "PENDENTE", "EM APROVACAO": "PENDENTE"})

    df.loc[num_doc_ok.notna() & tipo_doc_ok.isna(), "STATUS_FLUXO"] = "AGUARDANDO CLASSIFICAÇÃO"
    df.loc[num_doc_ok.notna() & tipo_doc_ok.notna(), "STATUS_FLUXO"] = "CLASSIFICADO"

    color_map = {
        "APROVADO": "#1f9d55",
        "PENDENTE": "#f59e0b",
        "AGUARDANDO CLASSIFICAÇÃO": "#515053",
        "CLASSIFICADO": "#2563EB",
    }

    # cor do texto do status dentro do tooltip
    def status_html(st):
        st = str(st).upper().strip()
        cor = color_map.get(st, "#0f172a")
        return f"<span style='color:{cor}; font-weight:700'>{st}</span>"

    df["VALOR_FORMATADO"] = df["VALOR_TOTAL"].apply(format_rs)
    df["STATUS_HTML"] = df["STATUS_FLUXO"].apply(status_html)

    # ordem do eixo X
    categoryarray = (
        df.dropna(subset=["NIVEL_NUM"])[["NIVEL_NUM", "NIVEL_LABEL"]]
        .drop_duplicates()
        .sort_values("NIVEL_NUM")["NIVEL_LABEL"].tolist()
    )

    custom_cols = [
        "NUM_PEDIDO", "NOME_FORNECEDOR", "COD_FORNECEDOR","C7_DESCRI",
        "CENTRO_CUSTO", "DESCR_CC", "CONTRATO",
        "DT_EMISSAO", "DATA_VENCIMENTO",
        "VALOR_FORMATADO", "NUM_DOCUMENTO", "TIPO_DOCUMENTO",
        "STATUS_HTML", "NOME_APROVADOR","NOME_REQUISITANTE"
    ]
    for c in custom_cols:
        if c not in df.columns:
            df[c] = ""

    fig = px.scatter(
        df,
        x="NIVEL_LABEL",
        y="NUM_PEDIDO",
        color="STATUS_FLUXO",
        color_discrete_map=color_map,
        category_orders={"NIVEL_LABEL": categoryarray},
        custom_data=custom_cols,
    )

    fig.update_traces(
        marker=dict(size=12, line=dict(width=0.8, color="rgba(0,0,0,.25)")),
        hovertemplate=(
            "<b>📄 Pedido:</b> %{customdata[0]}<br>"
            "<b>🏢 Fornecedor:</b> %{customdata[1]} (%{customdata[2]})<br>"
            "<b>📦 Produto:</b> %{customdata[3]}<br>"
            "<b>🏷 Centro de Custo:</b> %{customdata[4]}<br>"
            "<b>📝 Descrição CC:</b> %{customdata[5]}<br>"
            "<b>📑 Contrato:</b> %{customdata[6]}<br>"
            "<br><span style='color:#94a3b8'>──────────────</span><br>"
            "<b>📅 Emissão:</b> %{customdata[7]} | "
            "<b>⏳ Venc.:</b> %{customdata[8]}<br>"
            "<b>💰 Valor:</b> %{customdata[9]}<br>"
            "<b>🧾 Documento:</b> %{customdata[10]} | "
            "<b>Tipo:</b> %{customdata[11]}<br>"
            "<b>🔔 Status:</b> %{customdata[12]}<br>"
            "<b>👤 Aprovador:</b> %{customdata[13]}<br>"
            "<b>👥 Requisitante:</b> %{customdata[14]}<br>"
            "<extra></extra>"
        ),
    )

    fig.update_layout(
        hovermode="closest",
        hoverlabel=dict(bgcolor="white", bordercolor="#cbd5e1", font=dict(size=12, color="#0f172a"), align="left"),
        xaxis_title="Nível",
        yaxis_title="Pedido",
        legend_title_text="Status",
        margin=dict(l=10, r=10, t=10, b=10),
    )
    fig.update_xaxes(categoryorder="array", categoryarray=categoryarray)
    return fig

def build_figures(dff: pd.DataFrame):
    if dff.empty:
        empty = px.bar(pd.DataFrame({"x": [], "y": []}), x="x", y="y")
        empty.update_layout(title=None)
        return empty, empty, empty, empty, build_timeline_figure(dff)

    st = (
        dff["STATUS_APROVACAO"].fillna("N/I").astype(str).replace({"": "N/I"})
        .value_counts(dropna=False).rename_axis("STATUS").reset_index(name="QTD")
    )
    fig_status = px.bar(st, x="STATUS", y="QTD")
    fig_status.update_layout(title=None)

    nv = (
        dff["NIVEL"].fillna("N/I").astype(str).replace({"": "N/I"})
        .value_counts().rename_axis("NIVEL").reset_index(name="QTD").sort_values("NIVEL")
    )
    fig_nivel = px.bar(nv, x="NIVEL", y="QTD")
    fig_nivel.update_layout(title=None)

    pend = dff[dff["STATUS_APROVACAO"].eq("PENDENTE")] if "STATUS_APROVACAO" in dff.columns else dff.iloc[0:0]
    if len(pend) > 0:
        ap = (
            pend["NOME_APROVADOR"].fillna("N/I").astype(str).replace({"": "N/I"})
            .value_counts().rename_axis("APROVADOR").reset_index(name="QTD").head(15)
        )
        fig_aprov = px.bar(ap, x="QTD", y="APROVADOR", orientation="h", text="QTD")
        fig_aprov.update_layout(title=None, yaxis={"categoryorder": "total ascending"}, margin=dict(l=10, r=10, t=10, b=10))
    else:
        fig_aprov = px.bar(pd.DataFrame({"APROVADOR": ["N/I"], "QTD": [0]}), x="QTD", y="APROVADOR", orientation="h")
        fig_aprov.update_layout(title=None)

    if "DT_EMISSAO_DT" in dff.columns:
        tmp = dff.copy()
        tmp["DT_EMISSAO_DT"] = pd.to_datetime(tmp["DT_EMISSAO_DT"], errors="coerce")  # garante datetime
        tmp = tmp.dropna(subset=["DT_EMISSAO_DT"])

        tmp["PERIODO"] = tmp["DT_EMISSAO_DT"].dt.to_period("M").dt.to_timestamp()
        df_periodo = (
            tmp.groupby("PERIODO")["NUM_PEDIDO"]
            .nunique()
            .reset_index(name="QTD_PEDIDOS")
            .sort_values("PERIODO")
        )
    else:
        df_periodo = pd.DataFrame({"PERIODO": [pd.Timestamp.today().normalize()], "QTD_PEDIDOS": [0]})

    fig_periodo = px.area(df_periodo, x="PERIODO", y="QTD_PEDIDOS")
    fig_periodo.update_traces(mode="lines+markers", line_shape="spline", marker=dict(size=8), line=dict(width=2))
    fig_periodo.update_layout(title=None, xaxis_title="Período", yaxis_title="Pedidos", hovermode="x unified")

    fig_timeline = build_timeline_figure(dff)
    return fig_status, fig_nivel, fig_aprov, fig_periodo, fig_timeline

def build_produto_sd1_vertical(dff_main: pd.DataFrame, dff_sd1: pd.DataFrame, top_n: int = 10, width: int = 450):
    if dff_main.empty or dff_sd1.empty:
        fig = px.bar(pd.DataFrame({"Produto": [], "VALOR_SD1": []}), x="Produto", y="VALOR_SD1")
        fig.update_layout(template="plotly_white", title=None, width=width, height=420, margin=dict(l=14, r=14, t=10, b=90))
        return fig

    needed_main = {"NUM_DOCUMENTO", "C7_DESCRI"}
    needed_sd1 = {"NUM_DOCUMENTO", "VALOR_SD1"}
    if not needed_main.issubset(dff_main.columns) or not needed_sd1.issubset(dff_sd1.columns):
        fig = px.bar(
            pd.DataFrame({"Produto": [], "VALOR_SD1": []}),
            x="VALOR_SD1",
            y="Produto",
            orientation="v",
        )
        fig.update_layout(
            template="plotly_white",
            title=None,
            width=width,
            height=420,
            margin={"l": 14, "r": 14, "t": 10, "b": 90}
        )
        return fig

    # SD1 por documento
    sd1_doc = (
        dff_sd1.groupby("NUM_DOCUMENTO", as_index=False)["VALOR_SD1"]
        .sum()
        .rename(columns={"VALOR_SD1": "VALOR_SD1_DOC"})
    )

    # Documento -> Produto (MAIN)
    doc_prod = dff_main[["NUM_DOCUMENTO", "C7_DESCRI"]].dropna().copy()
    doc_prod["NUM_DOCUMENTO"] = doc_prod["NUM_DOCUMENTO"].astype(str).str.strip()
    doc_prod["C7_DESCRI"] = doc_prod["C7_DESCRI"].astype(str).str.strip()

    base = doc_prod.merge(sd1_doc, on="NUM_DOCUMENTO", how="inner")
    if base.empty:
        fig = px.bar(pd.DataFrame({"Produto": [], "VALOR_SD1": []}), x="Produto", y="VALOR_SD1")
        fig.update_layout(template="plotly_white", title=None, width=width, height=470, margin=dict(l=14, r=14, t=10, b=90))
        return fig

    # Top 10 por produto
    prod = (
        base.groupby("C7_DESCRI", as_index=False)["VALOR_SD1_DOC"]
        .sum()
        .sort_values("VALOR_SD1_DOC", ascending=False)
        .head(top_n)
        .rename(columns={"C7_DESCRI": "Produto_full", "VALOR_SD1_DOC": "VALOR_SD1"})
    )

    # nome curto (bem curto) + índice para evitar repetição visual
    def short_label(s: str, max_len: int = 12) -> str:
        s = str(s).strip()
        return s if len(s) <= max_len else (s[:max_len - 1] + "…")

    prod["Produto_curto"] = prod["Produto_full"].apply(short_label)
    prod["Produto_x"] = [f"{i+1:02d} - {p}" for i, p in enumerate(prod["Produto_curto"].tolist())]
    order = prod["Produto_x"].tolist()

    fig = px.bar(
        prod,
        x="Produto_x",
        y="VALOR_SD1",
        custom_data=["Produto_full"],
        category_orders={"Produto_x": order},
        orientation="v",
    )

    fig.update_traces(
        hovertemplate=(
            "<b>Produto:</b> %{customdata[0]}<br>"
            "<b>Valor SD1:</b> " + "%{y:,.2f}<br>"
            "<extra></extra>"
        ),
    )

    fig.update_layout(
        template="plotly_white",
        title=None,
        width=width,
        height=450,
        margin=dict(l=14, r=14, t=10, b=100),
        bargap=0.25,
        showlegend=False,
        font=dict(size=12),
    )

    fig.update_yaxes(
        title="Valor SD1",
        tickformat="~s",                # 500M / 1G / 3G etc
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False,
    )

    fig.update_xaxes(
        title="Produto (Top 10)",
        tickangle=45,
        tickfont=dict(size=10),
        automargin=True,
        categoryorder="array",
        categoryarray=order,
    )

    return fig

# =========================================================
# 5) PIVOT SD1
# =========================================================
def format_br_money(v):
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def montar_pivot_sd1(dff_sd1: pd.DataFrame, value_col="VALOR_FINANCEIRO") -> pd.DataFrame:
    if dff_sd1.empty:
        return pd.DataFrame(columns=["NOME_FORNECEDOR"])

    if value_col not in dff_sd1.columns:
        value_col = "VALOR_FINANCEIRO"

    pv = pd.pivot_table(
        dff_sd1,
        index="NOME_FORNECEDOR",
        columns="MES_VENCIMENTO",
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        margins=True,
        margins_name="Total Geral",
    ).reset_index()

    cols = pv.columns.tolist()
    base = ["NOME_FORNECEDOR"]
    meses = sorted([c for c in cols if c not in base and c != "Total Geral"])
    pv = pv[base + meses + (["Total Geral"] if "Total Geral" in cols else [])]

    if "Total Geral" in pv.columns:
        is_total = pv["NOME_FORNECEDOR"].eq("Total Geral")
        pv_data = pv[~is_total].copy().sort_values("Total Geral", ascending=False)
        pv_total = pv[is_total].copy()
        pv = pd.concat([pv_data, pv_total], ignore_index=True)

    for c in pv.columns:
        if c != "NOME_FORNECEDOR":
            pv[c] = pv[c].apply(format_br_money)

    return pv

# =========================================================
# 6) OPTIONS (boot) - usa um fetch inicial
# =========================================================
df0 = preparar_campos_main(get_data_main())

options_fornecedor = opts_from_series(df0.get("NOME_FORNECEDOR"))
options_cc = opts_from_series(df0.get("CENTRO_CUSTO"))
options_pedido = opts_from_series(df0.get("NUM_PEDIDO"))
options_mes = opts_from_series(df0.get("MES_EMISSAO"))
options_status = opts_from_series(df0.get("STATUS_APROVACAO"))
options_descr_cc = opts_from_series(df0.get("DESCR_CC"))
options_requisitante = opts_from_series(df0.get("NOME_REQUISITANTE"))
options_aprovador = opts_from_series(df0.get("NOME_APROVADOR"))
options_contrato = opts_from_series(df0.get("CONTRATO"))
options_documento = opts_from_series(df0.get("NUM_DOCUMENTO"))
options_produto = opts_from_series(df0.get("C7_DESCRI"))

# =========================================================
# 7) APP / LAYOUT
# =========================================================
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, BOOTSTRAP_ICONS])
app.title = APP_TITLE

sidebar = dbc.Card(
    dbc.CardBody(
        [
            dbc.Row(
                [
                    dbc.Col(html.H4("Filtros", className="mb-0",
                                    style={"fontWeight": "700", "color": "#07242b", "textAlign": "center"}), width=True),
                    dbc.Col(dbc.Button("≡", id="btn_sidebar", color="secondary", outline=True, size="sm", className="ms-auto"),
                            width="auto"),
                ],
                align="center",
                className="mb-1",
            ),
            dbc.Collapse(
                id="sidebar_collapse",
                is_open=True,
                children=[
                    html.Div("Fornecedor", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_fornecedor", options=options_fornecedor, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Centro de custo", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_cc", options=options_cc, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Descrição Centro de Custo", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_descr_cc", options=options_descr_cc, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Número do pedido", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_pedido", options=options_pedido, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Mês emissão", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_mes", options=options_mes, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Status aprovação", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_status", options=options_status, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Requisitante", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_requisitante", options=options_requisitante, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Aprovador", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_aprovador", options=options_aprovador, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Contrato", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_contrato", options=options_contrato, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    
                    html.Div("Número do documento", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_documento", options=options_documento, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    
                    html.Div("Métrica do Pivot SD1", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(
                        id="f_metric_pivot",
                        options=[
                            {"label": "Valor Financeiro (E2_VALOR)", "value": "VALOR_FINANCEIRO"},
                            {"label": "Saldo (E2_SALDO)", "value": "VALOR_SALDO"},
                            {"label": "Total SD1 (D1_TOTAL)", "value": "VALOR_SD1"},
                        ],
                        value="VALOR_FINANCEIRO",
                        clearable=False
                    ),
                    html.Hr(),
                    
                    html.Div("Produto", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_produto", options=options_produto, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                ],
            ),
        ]
    ),
    className="shadow-sm",
)

main_content = html.Div(
    [
        html.H4("Aprovações (Protheus)", className="mb-2",
                style={"fontWeight": "700", "color": "#07242b", "textAlign": "center"}),
        
        html.Div(id="status_atualizacao", style={
            "textAlign": "center",
            "fontSize": "12px",
            "color": "#0f172a",
            "marginBottom": "6px"
        }),
        dbc.Row(
            [
                dbc.Col(html.Div(id="kpi_total_pedidos"), md=3),
                dbc.Col(html.Div(id="kpi_pendentes"), md=3),
                dbc.Col(html.Div(id="kpi_aprovados"), md=3),
                dbc.Col(html.Div(id="kpi_niveis"), md=3),
            ],
            className="mt-2 g-3",
        ),

        dbc.Row(
            [
                dbc.Col(html.Div(id="kpi_fin_total"), md=4),
                dbc.Col(html.Div(id="kpi_fin_saldo"), md=4),
                dbc.Col(html.Div(id="kpi_fin_atraso"), md=4),
            ],
            className="mt-2 g-3",
        ),
        
        dbc.Row(
            [
                dbc.Col(card_com_header("Por Status", "g_status"), md=3),
                dbc.Col(card_com_header("Produto (Top 10 por Valor SD1)", "g_nivel"), md=3),
                dbc.Col(card_com_header("Pendências por Aprovador (Top 15)", "g_aprovador"), md=3),
                dbc.Col(card_com_header("Pedidos por Período", "g_periodo"), md=3),
            ],
            className="mt-2 g-2",
        ),

        dbc.Row(
            [
                dbc.Col(card_com_header("Fluxo de Aprovação por Pedido", "g_aging"), md=6),
                dbc.Col(card_com_header("Aging (Saldo por faixa)", "g_top_saldo"), md=6),
            ],
            className="mt-2 g-2",
        ),
        
        dbc.Row(
            [dbc.Col(card_com_header("Top Fornecedores (Saldo em aberto)", "g_timeline"), md=12)],
            className="mt-2 g-2",
        ),

                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Card(
                                [
                                    dbc.CardHeader(
                                        "Contas a Pagar (SD1) - Fornecedor x Mês de Vencimento",
                                        style=HEADER_STYLE
                                    ),

                                    # dbc.Button(
                                    #     "Exportar Pivot SD1",
                                    #     id="btn_export_pivot",
                                    #     color="primary",
                                    #     size="sm",
                                    #     outline=True,
                                    #     style={"margin": "6px", "marginLeft": "8px"},
                                    # ),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                dbc.Button(
                                                    "Exportar XLSX",
                                                    id="btn_export_pivot",
                                                    color="success",
                                                    size="sm",
                                                    outline=True,
                                                ),
                                                width="auto",
                                            ),
                                        ],
                                        justify="end",
                                        style={"padding": "6px 10px"},
                                    ),
                                    dcc.Download(id="download_pivot"),

                                    dbc.CardBody(
                                        dash_table.DataTable(
                                            id="tbl_sd1_pivot",
                                            page_size=12,
                                            style_table={"width": "100%", "minWidth": "100%", "overflowX": "auto"},
                                            style_cell=TABLE_CELL_STYLE,
                                            style_header=TABLE_HEADER_STYLE,
                                            style_data_conditional=[
                                                {"if": {"row_index": "odd"}, "backgroundColor": "#d9d9d9"},
                                                {"if": {"row_index": "even"}, "backgroundColor": "white"},
                                                {"if": {"filter_query": '{NOME_FORNECEDOR} = "Total Geral"'}, "fontWeight": "700"},
                                            ],
                                        ),
                                        style={"padding": "6px"},
                                    ),
                                ],
                                className="shadow-sm w-100",
                                style=CARD_STYLE,
                            ),
                            md=12,
                        )
                    ],
                    className="mt-2 g-2",
                ),

        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            html.H6("Tabela", className="mb-0", style={"textAlign": "center"}),
                                            width=True
                                        ),
                                        dbc.Col(
                                            dbc.Button(
                                                "Exportar XLSX",
                                                id="btn_export_xlsx",
                                                color="success",
                                                size="sm",
                                                outline=True,
                                            ),
                                            width="auto",
                                        ),
                                    ],
                                    align="center",
                                    className="mb-2",
                                ),
                                dash_table.DataTable(
                                    id="tbl",
                                    filter_action="native",
                                    sort_action="native",
                                    page_size=15,
                                    style_table={"width": "100%", "minWidth": "100%", "overflowX": "auto"},
                                    style_cell=TABLE_CELL_STYLE,
                                    style_header=TABLE_HEADER_STYLE,
                                    style_data_conditional=[
                                        {"if": {"row_index": "odd"}, "backgroundColor": "#d9d9d9"},
                                        {"if": {"row_index": "even"}, "backgroundColor": "white"},
                                    ],
                                ),
                            ]
                        ),
                        className="shadow-sm w-100",
                    ),
                    width=12,
                )
            ],
            className="mt-3",
        ),
    ]
)

app.layout = dbc.Container(
    fluid=True,
    style=PAGE_STYLE,
    children=[
        dcc.Store(id="sidebar_state", data={"open": True}),
        dcc.Store(id="store_main"),
        dcc.Store(id="store_sd1"),
        dcc.Download(id="download_xlsx"),
        dcc.Interval(id="interval_refresh", interval=REFRESH_MS, n_intervals=0),
        dbc.Row(
            [
                dbc.Col(sidebar, id="col_sidebar", width=2),
                dbc.Col(main_content, id="col_main", width=10),
            ],
            className="g-2",
        ),
    ],
)

# =========================================================
# 8) CALLBACKS
# =========================================================

# --- carrega do banco 1x por refresh e guarda no Store (MAIN + SD1)
@app.callback(
    Output("store_main", "data"),
    Output("store_sd1", "data"),
    Input("interval_refresh", "n_intervals"),
)
def carregar_dados(n):
    df_main = preparar_campos_main(get_data_main())
    df_sd1 = preparar_campos_sd1(get_data_sd1())
    return df_main.to_dict("records"), df_sd1.to_dict("records")


@app.callback(
    Output("sidebar_collapse", "is_open"),
    Output("col_sidebar", "width"),
    Output("col_main", "width"),
    Output("sidebar_state", "data"),
    Input("btn_sidebar", "n_clicks"),
    State("sidebar_state", "data"),
    prevent_initial_call=True,
)
def toggle_sidebar(n, st):
    is_open = st.get("open", True)
    is_open = not is_open

    # >>> AQUI você controla a largura do menu <<<
    col_sidebar = 2 if is_open else 0   # mude 2 -> 3 (mais largo) ou 1 (mais estreito)
    col_main = 10 if is_open else 12

    return is_open, col_sidebar, col_main, {"open": is_open}


@app.callback(
    Output("status_atualizacao", "children"),
    Output("kpi_total_pedidos", "children"),
    Output("kpi_pendentes", "children"),
    Output("kpi_aprovados", "children"),
    Output("kpi_niveis", "children"),
    Output("kpi_fin_total", "children"),
    Output("kpi_fin_saldo", "children"),
    Output("kpi_fin_atraso", "children"),
    Output("g_status", "figure"),
    Output("g_nivel", "figure"),
    Output("g_aprovador", "figure"),
    Output("g_periodo", "figure"),
    Output("g_aging", "figure"),
    Output("g_top_saldo", "figure"),
    Output("g_timeline", "figure"),
    Output("tbl", "data"),
    Output("tbl", "columns"),
    Output("tbl_sd1_pivot", "data"),
    Output("tbl_sd1_pivot", "columns"),
    Input("store_main", "data"),
    Input("store_sd1", "data"),
    Input("f_fornecedor", "value"),
    Input("f_cc", "value"),
    Input("f_descr_cc", "value"),
    Input("f_pedido", "value"),
    Input("f_mes", "value"),
    Input("f_status", "value"),
    Input("f_requisitante", "value"),
    Input("f_aprovador", "value"),
    Input("f_contrato", "value"),
    Input("f_documento", "value"), 
    Input("f_metric_pivot", "value"),
    Input("f_produto", "value"),
)
def update_all(store_main, store_sd1, f_fornecedor, f_cc, f_descr_cc, f_pedido, f_mes, f_status, f_requisitante, f_aprovador, f_contrato, f_documento, f_metric_pivot, f_produto):
    dff = pd.DataFrame(store_main) if store_main else pd.DataFrame()
    dff_sd1 = pd.DataFrame(store_sd1) if store_sd1 else pd.DataFrame()

    # ======================
    # FILTROS (MAIN)
    # ======================
    if not dff.empty:
        if f_fornecedor:
            dff = dff[dff["NOME_FORNECEDOR"].astype(str).isin([str(x) for x in f_fornecedor])]

        if f_cc:
            dff = dff[dff["CENTRO_CUSTO"].astype(str).isin([str(x) for x in f_cc])]

        if f_descr_cc:
            dff = dff[dff["DESCR_CC"].astype(str).isin([str(x) for x in f_descr_cc])]

        if f_pedido:
            dff = dff[dff["NUM_PEDIDO"].astype(str).isin([str(x) for x in f_pedido])]

        if f_mes and "MES_EMISSAO" in dff.columns:
            dff = dff[dff["MES_EMISSAO"].astype(str).isin([str(x) for x in f_mes])]

        # ✅ propaga MES_EMISSAO (MAIN) para SD1 via NUM_DOCUMENTO
        if (
            (not dff.empty)
            and f_mes
            and ("MES_EMISSAO" in dff.columns)
            and ("NUM_DOCUMENTO" in dff.columns)
            and ("NUM_DOCUMENTO" in dff_sd1.columns)
        ):
            docs_validos = (
                dff["NUM_DOCUMENTO"]
                .astype(str).str.strip()
                .replace({"": np.nan, "None": np.nan, "nan": np.nan})
                .dropna()
                .unique()
                .tolist()
            )

            if docs_validos:  # evita filtrar com lista vazia
                dff_sd1 = dff_sd1[dff_sd1["NUM_DOCUMENTO"].astype(str).str.strip().isin(docs_validos)]

        if f_status:
            norm = [str(x).strip().upper() for x in (f_status if isinstance(f_status, list) else [f_status])]
            dff = dff[dff["STATUS_APROVACAO"].astype(str).str.strip().str.upper().isin(norm)]

        if f_requisitante:
            dff = dff[dff["NOME_REQUISITANTE"].astype(str).isin([str(x) for x in f_requisitante])]

            #caso queira filtrar SD1 pelos documentos dos requisitantes selecionados no MAIN
        if (
            (not dff.empty)
            and f_requisitante
            and ("NUM_DOCUMENTO" in dff.columns)
            and ("NUM_DOCUMENTO" in dff_sd1.columns)
        ):
            docs_req = (
                dff["NUM_DOCUMENTO"]
                .astype(str).str.strip()
                .replace({"": np.nan, "None": np.nan, "nan": np.nan})
                .dropna()
                .unique()
                .tolist()
            )

            if docs_req:
                dff_sd1 = dff_sd1[
                    dff_sd1["NUM_DOCUMENTO"].astype(str).str.strip().isin(docs_req)
                ]
            else:
                # se não há documentos no MAIN após o filtro, zera SD1
                dff_sd1 = dff_sd1.iloc[0:0]
        
        if f_aprovador:
            dff = dff[dff["NOME_APROVADOR"].astype(str).isin([str(x) for x in f_aprovador])]

        if f_contrato:
            dff = dff[dff["CONTRATO"].astype(str).isin([str(x) for x in f_contrato])]

        # filtro SD1 por documento (se o usuário selecionar documento)
        if f_documento and "NUM_DOCUMENTO" in dff_sd1.columns:
            dff_sd1 = dff_sd1[dff_sd1["NUM_DOCUMENTO"].astype(str).isin([str(x) for x in f_documento])]

        # filtro MAIN por documento (se o usuário selecionar documento)
        if f_documento and "NUM_DOCUMENTO" in dff.columns:
            dff = dff[dff["NUM_DOCUMENTO"].astype(str).isin([str(x) for x in f_documento])]
                       
        q_sd1 = len(dff_sd1)

        valor_sd1_total = float(
            dff_sd1.get("VALOR_SALDO", 0).sum()
        ) if not dff_sd1.empty else 0.0
        
        if f_produto and "C7_DESCRI" in dff.columns:
            dff = dff[dff["C7_DESCRI"].astype(str).isin([str(x) for x in f_produto])]

        # propaga produto para SD1 via documento
        if (
            (not dff.empty)
            and f_produto
            and ("NUM_DOCUMENTO" in dff.columns)
            and ("NUM_DOCUMENTO" in dff_sd1.columns)
        ):
            docs_prod = (
                dff["NUM_DOCUMENTO"].astype(str).str.strip()
                .replace({"": np.nan, "None": np.nan, "nan": np.nan})
                .dropna().unique().tolist()
            )
            dff_sd1 = dff_sd1[dff_sd1["NUM_DOCUMENTO"].astype(str).str.strip().isin(docs_prod)] if docs_prod else dff_sd1.iloc[0:0]

    agora = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")
    q_main = br_int(len(dff))
    q_sd1 = br_int(len(dff_sd1))


    if dff_sd1 is None or dff_sd1.empty:
        sd1_qtde_linhas = 0
        sd1_qtde_docs = 0
        sd1_valor = 0.0
        total_fin = 0.0
        saldo_aberto = 0.0
    else:
        sd1_qtde_linhas = int(len(dff_sd1))
        sd1_qtde_docs = int(dff_sd1["NUM_DOCUMENTO"].nunique()) if "NUM_DOCUMENTO" in dff_sd1.columns else 0

        total_fin = float(pd.to_numeric(dff_sd1.get("VALOR_FINANCEIRO", 0), errors="coerce").fillna(0).sum())
        saldo_aberto = float(pd.to_numeric(dff_sd1.get("VALOR_SALDO", 0), errors="coerce").fillna(0).sum())

        # o valor do badge (defina o que quer mostrar)
        sd1_valor = saldo_aberto  # ou total_fin

    status_txt = html.Div(
        [
            "🟢 Atualizado em: ",
            html.B(agora),
            make_badge(f"MAIN: {br_int(len(dff))}", "info"),
            make_badge(f"SD1 Qtde: {br_int(sd1_qtde_linhas)}", "secondary"),
            make_badge(f"SD1 Docs: {br_int(sd1_qtde_docs)}", "secondary"),
            make_badge(f"E2 Saldo: {format_rs(sd1_valor)}", "success")
        ],
        style={"display": "flex", "gap": "6px", "justifyContent": "center"}
    )
    
    # KPIs
    total_pedidos = int(dff["NUM_PEDIDO"].nunique()) if "NUM_PEDIDO" in dff.columns else 0
    pendentes = int(dff["STATUS_APROVACAO"].eq("PENDENTE").sum()) if "STATUS_APROVACAO" in dff.columns else 0
    aprovados = int(dff["STATUS_APROVACAO"].eq("APROVADO").sum()) if "STATUS_APROVACAO" in dff.columns else 0
    niveis = int(dff["NIVEL"].nunique()) if "NIVEL" in dff.columns else 0

    k1 = kpi_card("Pedidos (distintos)", f"{total_pedidos:,}".replace(",", "."), icon="bi bi-receipt")
    k2 = kpi_card("Pendências (linhas)", f"{pendentes:,}".replace(",", "."), icon="bi bi-hourglass-split")
    k3 = kpi_card("Aprovados (linhas)", f"{aprovados:,}".replace(",", "."), icon="bi bi-check2-circle")
    k4 = kpi_card("Níveis (distintos)", f"{niveis:,}".replace(",", "."), icon="bi bi-diagram-3")

    fig_status, _fig_nivel, fig_aprov, fig_periodo, fig_timeline = build_figures(dff)
    fig_nivel = build_produto_sd1_vertical(dff, dff_sd1, top_n=10, width=450)

# ========= FIG AGING =========
    if dff_sd1.empty:
        fig_aging = px.bar(pd.DataFrame({"FAIXA": [], "VALOR_SALDO": []}), x="FAIXA", y="VALOR_SALDO")
    else:
        x = dff_sd1.copy()

        # garante datetime Series (e não numpy array)
        if "DATA_VENCIMENTO_DT" not in x.columns:
            x["DATA_VENCIMENTO_DT"] = pd.to_datetime(x.get("DATA_VENCIMENTO", None), dayfirst=True, errors="coerce")
        else:
            x["DATA_VENCIMENTO_DT"] = pd.to_datetime(x["DATA_VENCIMENTO_DT"], errors="coerce")

        x = x.dropna(subset=["DATA_VENCIMENTO_DT"]).copy()

        hoje = pd.Timestamp.today().normalize()

        # agora é datetime Series -> funciona
        x["DIAS"] = (x["DATA_VENCIMENTO_DT"] - hoje).dt.days

        # saldo só em aberto
        if "VALOR_SALDO" not in x.columns:
            x["VALOR_SALDO"] = 0.0
        x["VALOR_SALDO"] = pd.to_numeric(x["VALOR_SALDO"], errors="coerce").fillna(0)

        x = x[x["VALOR_SALDO"] > 0].copy()

        def faixa(d):
            if d < 0: return "Vencido"
            if d <= 7: return "0-7"
            if d <= 15: return "8-15"
            if d <= 30: return "16-30"
            return "31+"

        x["FAIXA"] = x["DIAS"].apply(faixa)

        agg = x.groupby("FAIXA", as_index=False)["VALOR_SALDO"].sum()
        order = ["Vencido", "0-7", "8-15", "16-30", "31+"]
        agg["FAIXA"] = pd.Categorical(agg["FAIXA"], categories=order, ordered=True)
        agg = agg.sort_values("FAIXA")

        agg["RS"] = agg["VALOR_SALDO"].apply(format_rs)
        fig_aging = px.bar(agg, x="FAIXA", y="VALOR_SALDO", text="RS")
        fig_aging.update_layout(title=None, xaxis_title="Faixa", yaxis_title="Saldo")
        fig_aging.update_traces(textposition="outside")


    # ========= TOP SALDO FORNECEDOR =========
    if dff_sd1.empty:
        fig_top = px.bar(pd.DataFrame({"Fornecedor": [], "Saldo": []}), x="Saldo", y="Fornecedor", orientation="h")
    else:
        top = (
            dff_sd1.groupby("NOME_FORNECEDOR", as_index=False)["VALOR_SALDO"].sum()
            .sort_values("VALOR_SALDO", ascending=False).head(15)
        )
        top["RS"] = top["VALOR_SALDO"].apply(format_rs)
        fig_top = px.bar(top, x="VALOR_SALDO", y="NOME_FORNECEDOR", orientation="h", text="RS")
        fig_top.update_layout(title=None, xaxis_title="Saldo", yaxis_title="")
        fig_top.update_traces(textposition="outside")
        
        
    # ======================
    # tabela principal
    if dff.empty:
        data = []
        columns = []
    else:
        preferidas = [
            "NUM_PEDIDO", "DT_EMISSAO", "MES_EMISSAO",
            "NOME_FORNECEDOR", "CENTRO_CUSTO", "DESCR_CC",
            "NIVEL", "NOME_APROVADOR", "STATUS_APROVACAO",
            "VALOR_TOTAL", "NOME_REQUISITANTE", "CONTRATO"
        ]
        preferidas = [c for c in preferidas if c in dff.columns]
        cols = preferidas + [c for c in dff.columns if c not in preferidas]
        view = dff[cols].copy()

        data = view.to_dict("records")
        columns = [{"name": c, "id": c} for c in view.columns]

    # ======================
    # PIVOT SD1
    # ======================
    if not dff_sd1.empty and f_fornecedor and "NOME_FORNECEDOR" in dff_sd1.columns:
        dff_sd1 = dff_sd1[dff_sd1["NOME_FORNECEDOR"].astype(str).isin([str(x) for x in f_fornecedor])]

    pivot = montar_pivot_sd1(dff_sd1, value_col=f_metric_pivot)
    pivot_data = pivot.to_dict("records")
    pivot_cols = [{"name": c, "id": c} for c in pivot.columns]

    hoje = pd.Timestamp.today().normalize()

    # garante datetime antes de comparar
    if "DATA_VENCIMENTO_DT" in dff_sd1.columns:
        venc = pd.to_datetime(dff_sd1["DATA_VENCIMENTO_DT"], errors="coerce")
    elif "DATA_VENCIMENTO" in dff_sd1.columns:
        venc = pd.to_datetime(dff_sd1["DATA_VENCIMENTO"], dayfirst=True, errors="coerce")
    else:
        venc = pd.Series([pd.NaT] * len(dff_sd1), index=dff_sd1.index)

    if "VALOR_SALDO" in dff_sd1.columns:
        saldo = pd.to_numeric(dff_sd1["VALOR_SALDO"], errors="coerce").fillna(0)
    else:
        saldo = pd.Series(0, index=dff_sd1.index, dtype="float64")

    atraso = (venc < hoje) & (saldo > 0)
    saldo_atraso = float(saldo.loc[atraso].sum())

    if dff_sd1.empty:
        total_fin = 0.0
        saldo_aberto = 0.0
    else:
        total_fin = float(pd.to_numeric(dff_sd1.get("VALOR_FINANCEIRO", 0), errors="coerce").fillna(0).sum())
        saldo_aberto = float(pd.to_numeric(dff_sd1.get("VALOR_SALDO", 0), errors="coerce").fillna(0).sum())

    k_fin1 = kpi_card("Custo total, no Período", format_rs(total_fin), icon="bi bi-calculator")
    k_fin2 = kpi_card("Saldo em aberto", format_rs(saldo_aberto), icon="bi bi-wallet2")
    k_fin3 = kpi_card("Vencido (saldo)", format_rs(saldo_atraso), icon="bi bi-exclamation-triangle")
    
    return (
        status_txt,
        k1, k2, k3, k4,
        k_fin1, k_fin2, k_fin3,
        fig_status, fig_nivel, fig_aprov, fig_periodo, 
        fig_timeline,
        fig_aging, fig_top,
        data, columns,
        pivot_data, pivot_cols
    )


@app.callback(
    Output("download_xlsx", "data"),
    Input("btn_export_xlsx", "n_clicks"),
    State("tbl", "data"),
    prevent_initial_call=True,
)
def exportar_xlsx(n_clicks, table_data):
    if not table_data:
        return None
    df_export = pd.DataFrame(table_data)
    return dcc.send_data_frame(df_export.to_excel, "export.xlsx", index=False, sheet_name="Dados")

@app.callback(
    Output("download_pivot", "data"),
    Input("btn_export_pivot", "n_clicks"),
    State("tbl_sd1_pivot", "data"),
    prevent_initial_call=True,
)
def exportar_pivot(n, rows):
    if not rows:
        return None
    df = pd.DataFrame(rows)
    return dcc.send_data_frame(df.to_excel, "pivot_sd1.xlsx", index=False, sheet_name="Pivot")

# =========================================================
# 9) RUN
# =========================================================
if __name__ == "__main__":
    app.run(debug=True, port=8058)
