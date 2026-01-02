import pandas as pd
import numpy as np
from dash import Dash, html, dcc, clientside_callback
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import dash_ag_grid as dag
import pyodbc

# =========================
# 1) CONEXÃO + QUERY
# =========================
# Nota: Ocultei a senha por segurança. Preencha novamente antes de rodar.
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.0.244,1433;"
    "DATABASE=FLUIG_COMPASA;"
    "UID=consulta;"
    "PWD=G@l@t@s2:20;" 
)

sql_query = """
WITH BD_DETALHES AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY documentid
            ORDER BY
                CASE
                    WHEN status = 'selectAnalise' THEN 0
                    WHEN status = 'finalizado'    THEN 1
                    ELSE 2
                END,
                [version] DESC
        ) AS rn
    FROM [FLUIG_COMPASA].[dbo].[ML001072]
),
UltimosProcessos AS (
    SELECT
        P.*,
        ROW_NUMBER() OVER (
            PARTITION BY P.NUM_PROCES
            ORDER BY P.ASSIGN_START_DATE DESC
        ) AS Linha
    FROM TAR_PROCES P
)
SELECT
    CASE
        WHEN PW.STATUS = 0 THEN 'ATIVO'
        WHEN PW.STATUS = 1 THEN 'CANCELADO'
        WHEN PW.STATUS = 2 THEN 'FINALIZADO'
        ELSE 'N ENCONTRADO'
    END AS STATUS,
    PW.NUM_PROCES,
    PW.START_DATE,
    PW.END_DATE,
    D.[ID],
    CASE
        WHEN D.[nm_tecAtual] IS NULL OR LTRIM(RTRIM(D.[nm_tecAtual])) = ''
        THEN COALESCE(L.FULL_NAME, U.LOGIN)
        ELSE D.[nm_tecAtual]
    END AS nm_tecAtual,
    D.[numSolFluig],
    D.status AS STATUS2,
    D.[nome_solicitante],
    D.[dt_emissao],
    D.[input1],
    D.[input2],
    D.[input3],
    D.[lb_urgencia],
    D.[lb_impacto],
    D.[nm_atribuicao],
    D.[descSolicitante],
    D.[orientacao],
    D.[solucao],
    UP.ASSIGN_START_DATE,
    UP.ASSIGN_END_DATE,
    UP.CD_MATRICULA,
    UP.IDI_STATUS,
    UP.DSL_OBS_TAR,
    U.LOGIN,
    COALESCE(L.FULL_NAME, U.LOGIN) AS FULL_NAME
FROM BD_DETALHES D
LEFT JOIN PROCES_WORKFLOW PW
    ON D.documentid = PW.NR_DOCUMENTO_CARD
LEFT JOIN UltimosProcessos UP
    ON UP.Linha = 1
   AND UP.NUM_PROCES = PW.NUM_PROCES
LEFT JOIN FDN_USERTENANT U
    ON UP.CD_MATRICULA = U.USER_CODE COLLATE DATABASE_DEFAULT
LEFT JOIN FDN_USER L
    ON L.USER_ID = U.USER_ID
WHERE D.rn = 1
ORDER BY D.documentid DESC;
"""




# =========================
# 2) HELPERS
# =========================
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

def count_df(dff: pd.DataFrame, col: str, label: str):
    if col not in dff.columns:
        return pd.DataFrame({label: [], "QTD": []})
    return (
        dff[col]
        .fillna("N/I")
        .astype(str)
        .replace({"": "N/I"})
        .value_counts(dropna=False)
        .rename_axis(label)
        .reset_index(name="QTD")
    )

def br_num(x, dec=0):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        x = 0
    return f"{x:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")

def kpi_body(titulo, valor, sub="", icon="bi bi-bar-chart"):
    return dbc.CardBody(
        [
            dbc.Row(
                [
                    dbc.Col(
                        html.I(className=f"{icon} text-primary", style={"fontSize": "34px"}),
                        width="auto",
                    ),
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
            )
        ],
        style={"height": "110px", "display": "flex", "alignItems": "center", "justifyContent": "center"},
    )

def preparar_campos(dff: pd.DataFrame) -> pd.DataFrame:
    dff["START_DATE"] = pd.to_datetime(dff.get("START_DATE"), errors="coerce")
    dff["END_DATE"] = pd.to_datetime(dff.get("END_DATE"), errors="coerce")
    dff["dt_emissao"] = pd.to_datetime(dff.get("dt_emissao"), errors="coerce", dayfirst=True)
    dff["MES_EMISSAO"] = dff["dt_emissao"].dt.to_period("M").astype(str).str.replace("-", "/")

    dff["STATUS"] = (
        dff["STATUS"]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"NAN": np.nan})
    )

    dff["SLA_PROCESSO"] = np.where(
        dff["STATUS"] == "FINALIZADO",
        (dff["END_DATE"] - dff["START_DATE"]).dt.days,
        (pd.Timestamp.now().normalize() - dff["START_DATE"]).dt.days + 1
    )
    dff["SLA_PROCESSO"] = pd.to_numeric(dff["SLA_PROCESSO"], errors="coerce").fillna(0).astype(int)

    dff["SLA_CHAMADO"] = (dff["END_DATE"] - dff["START_DATE"]).dt.total_seconds() / 86400
    dff["SLA_CHAMADO"] = pd.to_numeric(dff["SLA_CHAMADO"], errors="coerce").fillna(0).astype(int)

    return dff

# =========================
# COMPONENTES DE LAYOUT
# =========================

def card_com_header(titulo, graph_id):
    return dbc.Card(
        [
            dbc.CardHeader(titulo, style={"fontWeight": "600", "textAlign": "center", "padding": "6px 10px"}),
            dbc.CardBody(
                dcc.Graph(id=graph_id, config={"displayModeBar": False}),
                style={"padding": "6px"},
            ),
        ],
        className="shadow-sm w-100 h-100",
    )

# URLs dos Temas
THEME_FLATLY = "https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/flatly/bootstrap.min.css"
THEME_DARKLY = "https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/darkly/bootstrap.min.css"

# =========================
# 3) CARGA INICIAL
# =========================
df0 = pd.read_sql(sql_query, conn)
df0 = preparar_campos(df0)

options_solicitante = opts_from_series(df0.get("nome_solicitante"))
options_mes = opts_from_series(df0.get("MES_EMISSAO"))
options_input1 = opts_from_series(df0.get("input1"))
options_input2 = opts_from_series(df0.get("input2"))
options_atribuicao = opts_from_series(df0.get("nm_atribuicao"))
options_numsol = opts_from_series(df0.get("numSolFluig"))
options_status = opts_from_series(df0.get("STATUS"))
options_tecnico = opts_from_series(df0.get("nm_tecAtual"))

preferidas = [
    "STATUS", "NUM_PROCES", "START_DATE", "END_DATE", "SLA_PROCESSO",
    "MES_EMISSAO", "nome_solicitante", "nm_atribuicao",
    "nm_tecAtual", "input1", "input2", "lb_impacto",
    "descSolicitante", "orientacao", "solucao"
]
cols0 = [c for c in preferidas if c in df0.columns]
cols0 += [c for c in df0.columns if c not in cols0]
view0 = df0[cols0].copy()
rowData0 = view0.to_dict("records")
columnDefs0 = [{"headerName": c, "field": c, "filter": True, "sortable": True, "resizable": True} for c in view0.columns]

# =========================
# 4) APP / LAYOUT / CSS
# =========================
BOOTSTRAP_ICONS = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css"

app = Dash(__name__, external_stylesheets=[BOOTSTRAP_ICONS])
app.title = "Painel Suporte Técnico"

# CSS CUSTOMIZADO
CUSTOM_CSS = """
/* =========================================================
   BASE
========================================================= */
html, body { font-family: var(--bs-body-font-family); }

/* =========================================================
   AG GRID
========================================================= */
.ag-theme-alpine, .ag-theme-alpine-dark { font-family: var(--bs-body-font-family); }
.ag-theme-alpine .ag-header, .ag-theme-alpine-dark .ag-header { font-weight: 700 !important; }
.ag-theme-alpine-dark .row-even { background-color: #303030 !important; }
.ag-theme-alpine-dark .row-odd  { background-color: #3a3a3a !important; }
.ag-theme-alpine .row-even { background-color: #ffffff !important; }
.ag-theme-alpine .row-odd  { background-color: #f8f9fa !important; }


/* =========================================================
   DCC DROPDOWN – FIX DEFINITIVO (Dash 3.x)
   (React-Select antigo + novo)
========================================================= */

/* ---------- CAMPO FECHADO ---------- */
.dark-theme .Select-control,
.dark-theme .Select__control{
    background-color: #303030 !important;
    border: 1px solid #555 !important;
}

.dark-theme .Select-value-label,
.dark-theme .Select-value-label span,
.dark-theme .Select-input > input,
.dark-theme .Select__single-value,
.dark-theme .Select__input-container,
.dark-theme .Select__input-container input{
    color: #ffffff !important;
}

.dark-theme .Select-placeholder,
.dark-theme .Select__placeholder{
    color: #aaaaaa !important;
}

.dark-theme .Select-arrow,
.dark-theme .Select__indicator{
    color: #ffffff !important;
}

/* ---------- MENU ABERTO (normal + portal) ---------- */
.dark-theme .Select-menu-outer,
.dark-theme .Select__menu,
.dark-theme .Select__menu-portal{
    background-color: #e9ecef !important;   /* fundo claro do menu */
    border: 1px solid #555 !important;
}

/* ✅ OPÇÕES (texto preto) — cobre normal + portal */
.dark-theme .Select-menu-outer .Select-option,
.dark-theme .Select__menu .Select__option,
.dark-theme .Select__menu-portal .Select__option{
    background-color: #e9ecef !important;  /* cinza claro */
    color: #111111 !important;            /* preto */
}

/* Hover / foco */
.dark-theme .Select-menu-outer .Select-option.is-focused,
.dark-theme .Select__option--is-focused{
    background-color: #cfe2ff !important;
    color: #000000 !important;
}

/* Selecionado */
.dark-theme .Select-menu-outer .Select-option.is-selected,
.dark-theme .Select__option--is-selected{
    background-color: #9ec5fe !important;
    color: #000000 !important;
}

/* ---------- MULTI SELECT (tags) ---------- */
.dark-theme .Select--multi .Select-value,
.dark-theme .Select__multi-value{
    background-color: #0d6efd !important;
    border: none !important;
}

.dark-theme .Select--multi .Select-value-label,
.dark-theme .Select--multi .Select-value-icon,
.dark-theme .Select__multi-value__label,
.dark-theme .Select__multi-value__remove{
    color: #ffffff !important;
}

/* Itens do menu */
.dark-theme .Select-menu-outer .VirtualizedSelectOption,
.dark-theme .Select-menu-outer .VirtualizedSelectOption span {
    color: #000000 !important;            /* preto */
    background-color: #e9ecef !important; /* cinza claro */
}

/* Hover / foco */
.dark-theme .Select-menu-outer .VirtualizedSelectFocusedOption,
.dark-theme .Select-menu-outer .VirtualizedSelectFocusedOption span {
    color: #000000 !important;
    background-color: #cfe2ff !important;
}

/* (Opcional) item selecionado – mantém preto */
.dark-theme .Select-menu-outer .Select-option.is-selected,
.dark-theme .Select-menu-outer .Select-option.is-selected span,
.dark-theme .Select-menu-outer .VirtualizedSelectOption.is-selected,
.dark-theme .Select-menu-outer .VirtualizedSelectOption.is-selected span {
    color: #000000 !important;
    background-color: #9ec5fe !important;
}

"""

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
            html.Hr(),
            # SWITCH CLARO/ESCURO
            # dbc.Switch(
            #     id="theme_switch",
            #     value=False,
            #     className="mb-2",
            #     label="Modo Escuro (Darkly)",
            #     persistence=True,
            # ),
            dbc.Switch(
                id="theme_switch",
                value=False,
                className="mb-2",
                # Em vez de texto, passamos um Span contendo os ícones
                label=html.Span([
                    html.I(className="bi bi-sun me-2"),  # Ícone de Sol
                    html.I(className="bi bi-moon")       # Ícone de Lua
                ]),
                persistence=True,
            ),
            html.Hr(),

            dbc.Collapse(
                id="sidebar_collapse",
                is_open=True,
                children=[
                    html.Div("Solicitante", className="text-muted small"),
                    dcc.Dropdown(id="f_solicitante", options=options_solicitante, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    html.Div("Data solicitação (Mês emissão)", className="text-muted small"),
                    dcc.Dropdown(id="f_mes_emissao", options=options_mes, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    html.Div("Nº Solicitação", className="text-muted small"),
                    dcc.Dropdown(id="f_num_solicitacao", options=options_numsol, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    html.Div("Status", className="text-muted small"),
                    dcc.Dropdown(id="f_status", options=options_status, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    html.Div("Técnico Atual", className="text-muted small"),
                    dcc.Dropdown(id="f_tecnico", options=options_tecnico, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    html.Div("Grupo", className="text-muted small"),
                    dcc.Dropdown(id="f_input1", options=options_input1, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    html.Div("Subgrupo", className="text-muted small"),
                    dcc.Dropdown(id="f_input2", options=options_input2, multi=True, placeholder="Selecione..."),
                    html.Hr(),
                    html.Div("Atribuição", className="text-muted small"),
                    dcc.Dropdown(id="f_atribuicao", options=options_atribuicao, multi=True, placeholder="Selecione..."),
                ],
            ),
        ]
    ),
    className="shadow-sm h-100",
)

main_content = html.Div(
    [
        html.H4("Painel - Suporte Técnico (Fluig)", className="mb-2 text-center pt-2 fw-bold"),
        dbc.Row(
            [
                dbc.Col(dbc.Card(id="kpi_total", className="shadow-sm w-100"), md=3),
                dbc.Col(dbc.Card(id="kpi_sla", className="shadow-sm w-100"), md=3),
                dbc.Col(dbc.Card(id="kpi_media", className="shadow-sm w-100"), md=3),
                dbc.Col(dbc.Card(id="kpi_abertos", className="shadow-sm w-100"), md=3),
            ],
            className="mt-2 g-3",
        ),
        dbc.Row(
            [
                dbc.Col(card_com_header("Chamados por Status", "g_status"), md=3),
                dbc.Col(card_com_header("Impacto (lb_impacto)", "g_impacto"), md=3),
                dbc.Col(card_com_header("Técnicos por Chamados", "g_tecnico"), md=3),
                dbc.Col(card_com_header("Distribuição - Grupo", "g_input1"), md=3),
            ],
            className="mt-2 g-2",
        ),
        dbc.Row(
            [
                dbc.Col(card_com_header("Distribuição - Subgrupo", "g_input2"), md=4),
                dbc.Col(card_com_header("Quantidade por Período", "g_periodo"), md=4),
                dbc.Col(card_com_header("Quantidade por Solicitante (Top 15)", "g_solicitante"), md=4),
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
                                        dbc.Col(html.H6("Tabela (descSolicitante)", className="mb-0 text-center"), width=True),
                                        dbc.Col(
                                            dbc.Button("Exportar XLSX", id="btn_export_xlsx", color="success", size="sm", outline=True),
                                            width="auto",
                                        ),
                                    ],
                                    align="center",
                                    className="mb-2",
                                ),
                                dag.AgGrid(
                                    id="tbl_ag",
                                    className="ag-theme-alpine", 
                                    columnDefs=columnDefs0,
                                    rowData=rowData0,
                                    defaultColDef={
                                        "sortable": True, "filter": True, "resizable": True,
                                        "wrapText": True, "autoHeight": True,
                                        "cellStyle": {"lineHeight": "1.2", "paddingTop": "2px", "paddingBottom": "2px", "fontSize": "13px"},
                                    },
                                    rowClassRules={
                                        "row-even": "params.node.rowIndex % 2 === 0",
                                        "row-odd": "params.node.rowIndex % 2 === 1",
                                    },
                                    dashGridOptions={"rowHeight": 30, "headerHeight": 32, "animateRows": False},
                                    style={"height": "800px", "width": "100%"},
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

# ==================================================================
# ATENÇÃO AQUI: Mudamos para html.Div(id="main_wrapper") como raiz
# ==================================================================
app.layout = html.Div(
    id="main_wrapper", # <--- Wrapper seguro para receber a classe CSS
    children=[
        html.Link(href=THEME_FLATLY, rel="stylesheet", id="theme_link"),
        dcc.Markdown(f"<style>{CUSTOM_CSS}</style>", dangerously_allow_html=True),
        dcc.Store(id="sidebar_state", data={"open": True}),
        dcc.Download(id="download_xlsx"),
        dcc.Interval(id="interval_refresh", interval=2 * 60 * 1000, n_intervals=0),
        
        # O container Bootstrap agora está DENTRO da Div Wrapper
        dbc.Container(
            fluid=True,
            children=[
                dbc.Row(
                    [
                        dbc.Col(sidebar, id="col_sidebar", width=2),
                        dbc.Col(main_content, id="col_main", width=10),
                    ],
                    className="g-2 pt-2",
                ),
            ],
            style={"minHeight": "100vh"}, 
        )
    ]
)

# =========================
# CALLBACK: TROCA CSS (TEMA)
# =========================
app.clientside_callback(
    """
function(value) {
    const flatly = "https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/flatly/bootstrap.min.css";
    const darkly = "https://cdn.jsdelivr.net/npm/bootswatch@5.3.2/dist/darkly/bootstrap.min.css";

    // aplica em HTML e BODY (pega dropdown mesmo quando “escapa” do wrapper)
    const isDark = !!value;

    document.documentElement.setAttribute("data-bs-theme", isDark ? "dark" : "light");
    document.documentElement.classList.toggle("dark-theme", !!value);
    document.documentElement.classList.toggle("light-theme", !value);

    document.body.classList.toggle("dark-theme", isDark);
    document.body.classList.toggle("light-theme", !isDark);

    return [isDark ? darkly : flatly, isDark ? "dark-theme" : "light-theme"];
}
    """,
    [
        Output("theme_link", "href"), 
        Output("main_wrapper", "className") # <--- Alvo agora é a Div, usando className (padrão)
    ],
    Input("theme_switch", "value")
)

# =========================
# Sidebar toggle
# =========================
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

    col_sidebar = 2 if is_open else 0
    col_main = 10 if is_open else 12

    return is_open, col_sidebar, col_main, {"open": is_open}

# =========================
# Callback Principal
# =========================
@app.callback(
    Output("kpi_total", "children"),
    Output("kpi_sla", "children"),
    Output("kpi_media", "children"),
    Output("kpi_abertos", "children"),
    Output("g_status", "figure"),
    Output("g_impacto", "figure"),
    Output("g_tecnico", "figure"),
    Output("g_input1", "figure"),
    Output("g_input2", "figure"),
    Output("g_periodo", "figure"),
    Output("g_solicitante", "figure"),
    Output("tbl_ag", "rowData"),
    Output("tbl_ag", "columnDefs"),
    Output("tbl_ag", "className"),
    Input("interval_refresh", "n_intervals"),
    Input("f_solicitante", "value"),
    Input("f_mes_emissao", "value"),
    Input("f_num_solicitacao", "value"),
    Input("f_status", "value"),
    Input("f_tecnico", "value"),
    Input("f_input1", "value"),
    Input("f_input2", "value"),
    Input("f_atribuicao", "value"),
    Input("theme_switch", "value"),
)
def update_all(n_intervals, f_solicitante, f_mes, f_numsol, f_status, f_tecnico, f_in1, f_in2, f_attr, is_dark_mode):
    
    # Configuração de Cores para Gráficos
    template = "plotly_dark" if is_dark_mode else "plotly"
    grid_class = "ag-theme-alpine-dark" if is_dark_mode else "ag-theme-alpine"
    
    # Se for dark, deixamos fundo transparente nos gráficos
    bg_color = "rgba(0,0,0,0)" if is_dark_mode else "#ffffff"

    dff = pd.read_sql(sql_query, conn)
    dff = preparar_campos(dff)

    # ... Filtros (código original) ...
    if f_solicitante:
        dff = dff[dff["nome_solicitante"].astype(str).isin([str(x) for x in f_solicitante])]
    if f_mes:
        dff = dff[dff["MES_EMISSAO"].astype(str).isin([str(x) for x in f_mes])]
    if f_numsol:
        dff = dff[dff["numSolFluig"].astype(str).isin([str(x) for x in f_numsol])]
    if isinstance(f_status, str): f_status = [f_status]
    if f_status:
        f_status_norm = [str(x).strip().upper() for x in f_status]
        dff = dff[dff["STATUS"].astype(str).str.strip().str.upper().isin(f_status_norm)]
    if isinstance(f_tecnico, str): f_tecnico = [f_tecnico]
    if f_tecnico:
        f_tecnico_norm = [str(x).strip() for x in f_tecnico]
        dff = dff[dff["nm_tecAtual"].astype(str).str.strip().isin(f_tecnico_norm)]
    if f_in1:
        dff = dff[dff["input1"].astype(str).isin([str(x) for x in f_in1])]
    if f_in2:
        dff = dff[dff["input2"].astype(str).isin([str(x) for x in f_in2])]
    if f_attr:
        dff = dff[dff["nm_atribuicao"].astype(str).isin([str(x) for x in f_attr])]

    total = len(dff)
    sla_proc_media = float(dff["SLA_PROCESSO"].dropna().mean()) if "SLA_PROCESSO" in dff.columns else 0.0
    meses_distintos = int(dff["MES_EMISSAO"].dropna().nunique()) if "MES_EMISSAO" in dff.columns else 0
    qtde_media = (total / meses_distintos) if meses_distintos > 0 else total
    chamados_abertos = dff["END_DATE"].isna().sum()

    k1 = kpi_body("Qtde Solicitações", f"{total:,}".replace(",", "."), icon="bi bi-ticket-perforated")
    k2 = kpi_body("SLA Processo (média - dias)", br_num(sla_proc_media, 0), icon="bi bi-clock-history")
    k3 = kpi_body("Qtde média (por mês)", br_num(qtde_media, 0), f"Meses no filtro: {meses_distintos}", icon="bi bi-calendar3")
    k4 = kpi_body("Chamados em Aberto", f"{chamados_abertos:,}".replace(",", "."), icon="bi bi-exclamation-circle")

    def update_fig(fig):
        fig.update_layout(
            paper_bgcolor=bg_color,
            plot_bgcolor=bg_color,
            margin=dict(l=10, r=10, t=30, b=10),
            title=None
        )
        return fig

    st = count_df(dff, "STATUS", "STATUS").sort_values("QTD", ascending=False)

    fig_status = px.bar(
        st,
        x="STATUS",
        y="QTD",
        text="QTD",             # ✅ mostra o valor
        template=template
    )

    # ✅ formata/posiciona o texto em cima das barras e melhora leitura
    fig_status.update_traces(
        texttemplate="%{text}",  # pode trocar por "%{text:,}" se quiser milhar
        textposition="outside",
        cliponaxis=False
    )

    # ✅ dá folga no eixo Y para não cortar o texto no topo
    fig_status.update_layout(
        uniformtext_minsize=10,
        uniformtext_mode="hide",
        yaxis=dict(rangemode="tozero"),
        margin=dict(l=10, r=10, t=30, b=10),
    )

    update_fig(fig_status)
    

    imp = count_df(dff, "lb_impacto", "Impacto")
    fig_impacto = px.pie(imp, names="Impacto", values="QTD", hole=0.6, template=template)
    update_fig(fig_impacto)

    tec_all = count_df(dff, "nm_tecAtual", "Técnico").sort_values("QTD", ascending=False)
    top15 = tec_all.head(15).copy()
    outros_qtd = tec_all["QTD"].iloc[15:].sum()
    if outros_qtd > 0:
        top15 = pd.concat([top15, pd.DataFrame([{"Técnico": "OUTROS", "QTD": outros_qtd}])], ignore_index=True)

    fig_tecnico = px.bar(top15, x="QTD", y="Técnico", orientation="h", text="QTD", template=template)
    fig_tecnico.update_layout(yaxis={"categoryorder": "total ascending"})
    update_fig(fig_tecnico)

    in1 = count_df(dff, "input1", "Grupo").sort_values("QTD", ascending=False).head(15)
    fig_in1 = px.scatter(in1, x="Grupo", y="QTD", size="QTD", color="Grupo", size_max=30, template=template)
    fig_in1.update_layout(showlegend=False)
    update_fig(fig_in1)

    in2 = count_df(dff, "input2", "Subgrupo").sort_values("QTD", ascending=False).head(20)
    fig_in2 = px.scatter(in2, x="Subgrupo", y="QTD", size="QTD", color="Subgrupo", size_max=30, template=template)
    fig_in2.update_layout(showlegend=False)
    update_fig(fig_in2)

    sol = (
        dff["nome_solicitante"].fillna("N/I").astype(str).str.strip().replace({"": "N/I"})
        .value_counts().reset_index()
    )
    sol.columns = ["Solicitante", "QTD"]
    top15_sol = sol.sort_values("QTD", ascending=False).head(15)
    fig_solicitante = px.bar(top15_sol, x="Solicitante", y="QTD", text="QTD", template=template)
    fig_solicitante.update_layout(xaxis_tickangle=-45)
    update_fig(fig_solicitante)

    tmp = dff.dropna(subset=["dt_emissao"]).copy()
    tmp["PERIODO"] = tmp["dt_emissao"].dt.to_period("M").dt.to_timestamp()
    df_periodo = tmp.groupby("PERIODO").size().reset_index(name="QTD").sort_values("PERIODO")
    fig_periodo = px.area(df_periodo, x="PERIODO", y="QTD", template=template)
    fig_periodo.update_traces(mode="lines+markers", line_shape="spline", marker=dict(size=8), line=dict(width=2))
    fig_periodo.update_layout(xaxis_title="Período", yaxis_title="Quantidade", hovermode="x unified")
    update_fig(fig_periodo)

    cols = [c for c in preferidas if c in dff.columns]
    cols += [c for c in dff.columns if c not in cols]
    view = dff[cols].copy()

    rowData = view.to_dict("records")
    columnDefs = [{"headerName": c, "field": c, "filter": True, "sortable": True, "resizable": True} for c in view.columns]

    return (
        k1, k2, k3, k4,
        fig_status, fig_impacto, fig_tecnico, fig_in1, fig_in2, fig_periodo, fig_solicitante,
        rowData, columnDefs, grid_class
    )

# =========================
# Export XLSX
# =========================
@app.callback(
    Output("download_xlsx", "data"),
    Input("btn_export_xlsx", "n_clicks"),
    State("tbl_ag", "rowData"),
    prevent_initial_call=True,
)
def exportar_xlsx(n_clicks, table_data):
    if not table_data:
        return None
    df_export = pd.DataFrame(table_data)
    return dcc.send_data_frame(
        df_export.to_excel,
        "suporte_tecnico.xlsx",
        index=False,
        sheet_name="Dados"
    )
    
if __name__ == "__main__":
    app.run(debug=True, port=8057)