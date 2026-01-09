import pandas as pd
import numpy as np
import plotly.graph_objects as go

from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
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
# 1) CONEXÃO / QUERY
# =========================================================
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.0.244,1433;"
    "DATABASE=P12_PRODUCAO;"
    "UID=consulta;"
    "PWD=G@l@t@s2:20;"
)
# OBS: removi colunas repetidas (AL_NIVEL/AL_APROV duplicadas) e aspas desnecessárias
sql_query = """
SELECT DISTINCT
    SAL.AL_COD AS COD_GRUPO_APROVADOR,
    C7.C7_NUM AS NUM_PEDIDO,
    C7.C7_NUMSC AS NUM_SOLICITACAO,
    C7.C7_EMISSAO AS DT_EMISSAO,
    C7.C7_CC AS CENTRO_CUSTO,
    CC.CTT_DESC01 AS DESCR_CC,
    C7.C7_DESCRI,
    C7.C7_FORNECE,
    C7.C7_LOJA,
    C7.C7_CONTRA AS CONTRATO,
    C7.C7_TOTAL AS VALOR_TOTAL,
    C7.C7_MEDICAO,
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
    ON C7_USER = SU.USR_ID
WHERE
    USR.D_E_L_E_T_ = ''
    AND SAL.AL_MSBLQL = '2'
    AND C7.C7_EMISSAO >= DATEADD(MONTH, -4, GETDATE())
ORDER BY SAL.AL_NIVEL ASC;
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

def br_num(x, dec=0):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        x = 0
    return f"{x:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")

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

# =========================================================
# 3) DATA LAYER
# =========================================================
def get_data() -> pd.DataFrame:
    with pyodbc.connect(CONN_STR) as conn:
        df = pd.read_sql(sql_query, conn)
    return df

def preparar_campos(dff: pd.DataFrame) -> pd.DataFrame:
    # Padroniza colunas
    if "DT_EMISSAO" in dff.columns:
        dff["DT_EMISSAO"] = pd.to_datetime(dff["DT_EMISSAO"], errors="coerce")
        dff["MES_EMISSAO"] = dff["DT_EMISSAO"].dt.to_period("M").astype(str).str.replace("-", "/")

    if "STATUS_APROVACAO" in dff.columns:
        dff["STATUS_APROVACAO"] = (
            dff["STATUS_APROVACAO"].astype(str).str.strip().str.upper().replace({"NAN": np.nan})
        )

    # garante tipos “amigáveis” para filtros
    for c in ["NUM_PEDIDO", "NOME_FORNECEDOR", "CENTRO_CUSTO", "NOME_APROVADOR", "DESCR_CC", "NIVEL", "NOME_REQUISITANTE"]:
        if c in dff.columns:
            dff[c] = dff[c].astype(str).str.strip()

    return dff

# =========================================================
# 4) FIGURES (ajustadas ao seu DF)
# =========================================================
def _to_int_or_nan(x):
    try:
        return int(str(x).strip())
    except Exception:
        return np.nan

def _aprovador_atual_por_pedido(g: pd.DataFrame) -> str:
    g2 = g.copy()
    g2["NIVEL_NUM"] = g2["NIVEL"].apply(_to_int_or_nan)
    g2 = g2.sort_values("NIVEL_NUM", na_position="last")

    pend = g2[g2["STATUS_APROVACAO"].astype(str).str.upper().isin(["PENDENTE", "AGUARDANDO", "EM APROVACAO"])]
    if len(pend) > 0:
        return str(pend.iloc[0].get("NOME_APROVADOR", "")).strip()

    return ""  # <- não mostra nada

def build_timeline_figure(dff: pd.DataFrame, max_pedidos: int = 40, max_nomes_por_nivel: int = 3):
    if dff.empty:
        return px.scatter(pd.DataFrame({"NIVEL": [], "NUM_PEDIDO": []}), x="NIVEL", y="NUM_PEDIDO")

    df = dff.copy()

    # NIVEL numérico (para ordenar)
    df["NIVEL_NUM"] = df["NIVEL"].apply(_to_int_or_nan)

    # --- monta nomes por nível (Top N por frequência) ---
    tmp_names = df.copy()
    tmp_names["NOME_APROVADOR"] = (
        tmp_names["NOME_APROVADOR"].astype(str).str.strip()
        .replace({"": np.nan, "None": np.nan, "nan": np.nan})
    )

    def pick_names(s: pd.Series) -> str:
        s = s.dropna().astype(str).str.strip()
        if s.empty:
            return ""
        vc = s.value_counts()
        nomes = vc.head(max_nomes_por_nivel).index.tolist()
        return "<br>".join(nomes)

    map_aprov_nivel = (
        tmp_names.dropna(subset=["NIVEL_NUM"])
                 .groupby("NIVEL_NUM")["NOME_APROVADOR"]
                 .apply(pick_names)
                 .to_dict()
    )

    # label por nível
    def mk_label(n):
        if pd.isna(n):
            return "N/I"
        n_int = int(n)
        nm = str(map_aprov_nivel.get(n_int, "")).strip()
        return f"{n_int} - {nm}" if nm else f"{n_int}"

    df["NIVEL_LABEL"] = df["NIVEL_NUM"].apply(mk_label)

    # --- limita pedidos (mais recentes) ---
    if "DT_EMISSAO" in df.columns:
        pedidos_ord = (
            df.dropna(subset=["DT_EMISSAO"])
              .sort_values("DT_EMISSAO", ascending=False)
              .groupby("NUM_PEDIDO", as_index=False)["DT_EMISSAO"].max()
              .sort_values("DT_EMISSAO", ascending=False)
        )
        pedidos_top = pedidos_ord["NUM_PEDIDO"].astype(str).head(max_pedidos).tolist()
        df = df[df["NUM_PEDIDO"].astype(str).isin(pedidos_top)]
    else:
        pedidos_top = df["NUM_PEDIDO"].astype(str).drop_duplicates().head(max_pedidos).tolist()
        df = df[df["NUM_PEDIDO"].astype(str).isin(pedidos_top)]

    # --- cores ---
    df["STATUS_APROVACAO"] = df["STATUS_APROVACAO"].astype(str).str.strip().str.upper()
    color_map = {"APROVADO": "#1f9d55", "PENDENTE": "#f59e0b"}  # verde / laranja

    # --- ORDEM FIXA DO EIXO X (1,2,3,4...) ---
    niveis_ordenados = (
        df.dropna(subset=["NIVEL_NUM"])[["NIVEL_NUM", "NIVEL_LABEL"]]
          .drop_duplicates()
          .sort_values("NIVEL_NUM")
    )
    categoryarray = niveis_ordenados["NIVEL_LABEL"].tolist()

    fig = px.scatter(
        df,
        x="NIVEL_LABEL",
        y="NUM_PEDIDO",
        color="STATUS_APROVACAO",
        color_discrete_map=color_map,
        category_orders={"NIVEL_LABEL": categoryarray},
        hover_data={
            "NIVEL": True,
            #"NOME_APROVADOR": True,
            "NOME_FORNECEDOR": True,
            "CENTRO_CUSTO": True,
            "NUM_PEDIDO": True,
            "CONTRATO": True,
            "C7_MEDICAO": True,
            "C7_DESCRI": True,
            "DESCR_CC": True,
            "DT_EMISSAO": True,
            "VALOR_TOTAL": True,
            "NIVEL_NUM": False,
        },
    )

    fig.update_traces(marker=dict(size=12, line=dict(width=0.8, color="rgba(0,0,0,.25)")))

    # garante a ordem mesmo se o plotly resolver “reordenar”
    fig.update_xaxes(categoryorder="array", categoryarray=categoryarray)

    fig.update_layout(
        title=None,
        xaxis_title="Nível",
        yaxis_title="Pedido",
        legend_title_text="Status",
        margin=dict(l=10, r=10, t=10, b=10),
        hovermode="closest",
    )

    return fig

def build_figures(dff: pd.DataFrame):
    # Status
    st = (
        dff["STATUS_APROVACAO"]
        .fillna("N/I")
        .astype(str)
        .replace({"": "N/I"})
        .value_counts(dropna=False)
        .rename_axis("STATUS")
        .reset_index(name="QTD")
    )
    fig_status = px.bar(st, x="STATUS", y="QTD")
    fig_status.update_layout(title=None)

    # Por nível
    if "NIVEL" in dff.columns:
        nv = (
            dff["NIVEL"]
            .fillna("N/I")
            .astype(str)
            .replace({"": "N/I"})
            .value_counts()
            .rename_axis("NIVEL")
            .reset_index(name="QTD")
            .sort_values("NIVEL")
        )
        fig_nivel = px.bar(nv, x="NIVEL", y="QTD")
        fig_nivel.update_layout(title=None)
    else:
        fig_nivel = px.bar(pd.DataFrame({"NIVEL": ["N/I"], "QTD": [1]}), x="NIVEL", y="QTD")
        fig_nivel.update_layout(title=None)

    # Top aprovadores (pendentes)
    pend = dff[dff["STATUS_APROVACAO"].eq("PENDENTE")] if "STATUS_APROVACAO" in dff.columns else dff.iloc[0:0]
    if len(pend) > 0:
        ap = (
            pend["NOME_APROVADOR"]
            .fillna("N/I")
            .astype(str)
            .replace({"": "N/I"})
            .value_counts()
            .rename_axis("APROVADOR")
            .reset_index(name="QTD")
            .head(15)
        )
        fig_aprov = px.bar(ap, x="QTD", y="APROVADOR", orientation="h", text="QTD")
        fig_aprov.update_layout(title=None, yaxis={"categoryorder": "total ascending"}, margin=dict(l=10, r=10, t=10, b=10))
    else:
        fig_aprov = px.bar(pd.DataFrame({"APROVADOR": ["N/I"], "QTD": [0]}), x="QTD", y="APROVADOR", orientation="h")
        fig_aprov.update_layout(title=None)

    # Período
    if "DT_EMISSAO" in dff.columns:
        tmp = dff.dropna(subset=["DT_EMISSAO"]).copy()
        tmp["PERIODO"] = tmp["DT_EMISSAO"].dt.to_period("M").dt.to_timestamp()
        df_periodo = tmp.groupby("PERIODO")["NUM_PEDIDO"].nunique().reset_index(name="QTD_PEDIDOS").sort_values("PERIODO")
    else:
        df_periodo = pd.DataFrame({"PERIODO": [pd.Timestamp.today().normalize()], "QTD_PEDIDOS": [0]})

    fig_periodo = px.area(df_periodo, x="PERIODO", y="QTD_PEDIDOS")
    fig_periodo.update_traces(mode="lines+markers", line_shape="spline", marker=dict(size=8), line=dict(width=2))
    fig_periodo.update_layout(title=None, xaxis_title="Período", yaxis_title="Pedidos", hovermode="x unified")

    fig_timeline = build_timeline_figure(dff)
    
    return fig_status, fig_nivel, fig_aprov, fig_periodo, fig_timeline

# =========================================================
# 5) OPTIONS (boot)
# =========================================================
df0 = preparar_campos(get_data())

options_fornecedor = opts_from_series(df0.get("NOME_FORNECEDOR"))
options_cc = opts_from_series(df0.get("CENTRO_CUSTO"))
options_pedido = opts_from_series(df0.get("NUM_PEDIDO"))
options_mes = opts_from_series(df0.get("MES_EMISSAO"))
options_status = opts_from_series(df0.get("STATUS_APROVACAO"))
options_descr_cc = opts_from_series(df0.get("DESCR_CC"))
options_requisitante = opts_from_series(df0.get("NOME_REQUISITANTE"))
options_aprovador = opts_from_series(df0.get("NOME_APROVADOR"))

# =========================================================
# 6) APP / LAYOUT
# =========================================================
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, BOOTSTRAP_ICONS])
app.title = APP_TITLE

sidebar = dbc.Card(
    dbc.CardBody(
        [
            dbc.Row(
                [
                    dbc.Col(html.H5("Filtros", className="mb-0"), width=True),
                    dbc.Col(
                        dbc.Button("≡", id="btn_sidebar", color="secondary", outline=True, size="sm", className="ms-auto"),
                        width="auto",
                    ),
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
                dbc.Col(card_com_header("Por Status", "g_status"), md=3),
                dbc.Col(card_com_header("Por Nível", "g_nivel"), md=3),
                dbc.Col(card_com_header("Pendências por Aprovador (Top 15)", "g_aprovador"), md=3),
                dbc.Col(card_com_header("Pedidos por Período", "g_periodo"), md=3),
            ],
            className="mt-2 g-2",
        ),
        dbc.Row(
            [
                dbc.Col(card_com_header("Fluxo de Aprovação por Pedido", "g_timeline"), md=12),
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
                                        dbc.Col(html.H6("Tabela", className="mb-0", style={"textAlign": "center"}), width=True),
                                        dbc.Col(
                                            dbc.Button("Exportar XLSX", id="btn_export_xlsx", color="success", size="sm", outline=True),
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
# 7) CALLBACKS
# =========================================================
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
    col_sidebar = 3 if is_open else 0
    col_main = 9 if is_open else 12
    return is_open, col_sidebar, col_main, {"open": is_open}

@app.callback(
    Output("kpi_total_pedidos", "children"),
    Output("kpi_pendentes", "children"),
    Output("kpi_aprovados", "children"),
    Output("kpi_niveis", "children"),
    Output("g_status", "figure"),
    Output("g_nivel", "figure"),
    Output("g_aprovador", "figure"),
    Output("g_periodo", "figure"),
    Output("g_timeline", "figure"),
    Output("tbl", "data"),
    Output("tbl", "columns"),
    Input("interval_refresh", "n_intervals"),
    Input("f_fornecedor", "value"),
    Input("f_cc", "value"),
    Input("f_descr_cc", "value"),
    Input("f_pedido", "value"),
    Input("f_mes", "value"),
    Input("f_status", "value"),
    Input("f_requisitante", "value"),
    Input("f_aprovador", "value"),
)
def update_all(n_intervals, f_fornecedor, f_cc, f_descr_cc, f_pedido, f_mes, f_status, f_requisitante, f_aprovador):
    dff = preparar_campos(get_data())

    # filtros
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
    if f_status:
        norm = [str(x).strip().upper() for x in (f_status if isinstance(f_status, list) else [f_status])]
        dff = dff[dff["STATUS_APROVACAO"].astype(str).str.strip().str.upper().isin(norm)]
    if f_requisitante:
        dff = dff[dff["NOME_REQUISITANTE"].astype(str).isin([str(x) for x in f_requisitante])]
        
    if f_aprovador:
        dff = dff[dff["NOME_APROVADOR"].astype(str).isin([str(x) for x in f_aprovador])]    

    # KPIs
    total_pedidos = int(dff["NUM_PEDIDO"].nunique()) if "NUM_PEDIDO" in dff.columns else 0
    pendentes = int(dff["STATUS_APROVACAO"].eq("PENDENTE").sum()) if "STATUS_APROVACAO" in dff.columns else 0
    aprovados = int(dff["STATUS_APROVACAO"].eq("APROVADO").sum()) if "STATUS_APROVACAO" in dff.columns else 0
    niveis = int(dff["NIVEL"].nunique()) if "NIVEL" in dff.columns else 0

    k1 = kpi_card("Pedidos (distintos)", f"{total_pedidos:,}".replace(",", "."), icon="bi bi-receipt")
    k2 = kpi_card("Pendências (linhas)", f"{pendentes:,}".replace(",", "."), icon="bi bi-hourglass-split")
    k3 = kpi_card("Aprovados (linhas)", f"{aprovados:,}".replace(",", "."), icon="bi bi-check2-circle")
    k4 = kpi_card("Níveis (distintos)", f"{niveis:,}".replace(",", "."), icon="bi bi-diagram-3")

    # figs
    fig_status, fig_nivel, fig_aprov, fig_periodo, fig_timeline = build_figures(dff)

    # tabela (colunas principais primeiro)
    preferidas = [
        "NUM_PEDIDO", "DT_EMISSAO", "MES_EMISSAO",
        "NOME_FORNECEDOR", "CENTRO_CUSTO", "DESCR_CC",
        "NIVEL", "NOME_APROVADOR", "STATUS_APROVACAO",
        "VALOR_TOTAL","NOME_REQUISITANTE"
    ]
    preferidas = [c for c in preferidas if c in dff.columns]
    cols = preferidas + [c for c in dff.columns if c not in preferidas]
    view = dff[cols].copy()

    data = view.to_dict("records")
    columns = [{"name": c, "id": c} for c in view.columns]

    return k1, k2, k3, k4, fig_status, fig_nivel, fig_aprov, fig_periodo, fig_timeline, data, columns


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

# =========================================================
# 8) RUN
# =========================================================
if __name__ == "__main__":
    app.run(debug=True, port=8058)
