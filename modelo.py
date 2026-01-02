import pandas as pd
import numpy as np

from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px

import pyodbc

# =========================================================
# 0) CONFIG / CONSTANTES
# =========================================================
BOOTSTRAP_ICONS = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css"

APP_TITLE = "Template Dash - Compasa"
REFRESH_MS = 2 * 60 * 1000   # 2 min

# --- estilos do tema (mantém seu visual) ---
PAGE_STYLE = {"padding": "12px", "backgroundColor": "#a5aeb8", "minHeight": "100vh"}

HEADER_STYLE = {
    "backgroundColor": "#E9ECEF",
    "color": "#C02626",
    "fontWeight": "600",
    "textAlign": "center",
    "padding": "6px 10px",
    "borderBottom": "1px solid #CED4DA",
}

CARD_STYLE = {
    "border": "1px solid #CED4DA",
    "borderRadius": "10px",
    "overflow": "hidden",
}

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
# 1) CONEXÃO / QUERY (troque aqui quando for outro projeto)
# =========================================================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.0.244,1433;"
    "DATABASE=FLUIG_COMPASA;"
    "UID=consulta;"
    "PWD=G@l@t@s2:20;"
)

sql_query = """
-- troque por sua query
SELECT TOP 500
    GETDATE() as dt_emissao,
    'EXEMPLO' as STATUS,
    'Fulano' as nome_solicitante,
    'Time A' as nm_tecAtual,
    'Grupo 1' as input1,
    'Sub 1' as input2
"""

# =========================================================
# 2) HELPERS (reaproveitáveis)
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
            dbc.CardBody(
                dcc.Graph(id=graph_id, config={"displayModeBar": False}),
                style={"padding": "6px"},
            ),
        ],
        style=CARD_STYLE,
        className="shadow-sm w-100",
    )

# =========================================================
# 3) DATA LAYER (troque aqui por cache/ETL se quiser)
# =========================================================
def get_data() -> pd.DataFrame:
    df = pd.read_sql(sql_query, conn)
    return df

def preparar_campos(dff: pd.DataFrame) -> pd.DataFrame:
    # EXEMPLO de campos padrões (você ajusta por projeto)
    if "dt_emissao" in dff.columns:
        dff["dt_emissao"] = pd.to_datetime(dff["dt_emissao"], errors="coerce")
        dff["MES_EMISSAO"] = dff["dt_emissao"].dt.to_period("M").astype(str).str.replace("-", "/")
    if "STATUS" in dff.columns:
        dff["STATUS"] = dff["STATUS"].astype(str).str.strip().str.upper().replace({"NAN": np.nan})
    return dff

# =========================================================
# 4) FIGURES (troque aqui: seus gráficos do projeto)
# =========================================================
def build_figures(dff: pd.DataFrame):
    # Status
    st = count_df(dff, "STATUS", "STATUS")
    fig_status = px.bar(st, x="STATUS", y="QTD")
    fig_status.update_layout(title=None)

    # Impacto (se existir)
    if "lb_impacto" in dff.columns:
        imp = count_df(dff, "lb_impacto", "Impacto")
        fig_impacto = px.pie(imp, names="Impacto", values="QTD", hole=0.6)
        fig_impacto.update_layout(title=None)
    else:
        fig_impacto = px.pie(pd.DataFrame({"Impacto": ["N/I"], "QTD": [1]}), names="Impacto", values="QTD", hole=0.6)
        fig_impacto.update_layout(title=None)

    # Técnico
    tec_all = count_df(dff, "nm_tecAtual", "Técnico").sort_values("QTD", ascending=False)
    top15 = tec_all.head(15).copy()
    outros_qtd = tec_all["QTD"].iloc[15:].sum() if len(tec_all) > 15 else 0
    if outros_qtd > 0:
        top15 = pd.concat([top15, pd.DataFrame([{"Técnico": "OUTROS", "QTD": outros_qtd}])], ignore_index=True)
    fig_tecnico = px.bar(top15, x="QTD", y="Técnico", orientation="h", text="QTD")
    fig_tecnico.update_layout(title=None, yaxis={"categoryorder": "total ascending"}, margin=dict(l=10, r=10, t=10, b=10))

    # Grupo/Subgrupo
    in1 = count_df(dff, "input1", "Grupo").sort_values("QTD", ascending=False).head(15)
    fig_in1 = px.scatter(in1, x="Grupo", y="QTD", size="QTD", color="Grupo", size_max=30)
    fig_in1.update_layout(showlegend=False, title=None)

    in2 = count_df(dff, "input2", "Subgrupo").sort_values("QTD", ascending=False).head(20)
    fig_in2 = px.scatter(in2, x="Subgrupo", y="QTD", size="QTD", color="Subgrupo", size_max=30)
    fig_in2.update_layout(showlegend=False, title=None)

    # Período (área suavizada + pontos)
    if "dt_emissao" in dff.columns:
        tmp = dff.dropna(subset=["dt_emissao"]).copy()
        tmp["PERIODO"] = tmp["dt_emissao"].dt.to_period("M").dt.to_timestamp()
        df_periodo = tmp.groupby("PERIODO").size().reset_index(name="QTD").sort_values("PERIODO")
    else:
        df_periodo = pd.DataFrame({"PERIODO": [pd.Timestamp.today().normalize()], "QTD": [0]})

    fig_periodo = px.area(df_periodo, x="PERIODO", y="QTD")
    fig_periodo.update_traces(mode="lines+markers", line_shape="spline", marker=dict(size=8), line=dict(width=2))
    fig_periodo.update_layout(title=None, xaxis_title="Período", yaxis_title="Quantidade", hovermode="x unified")

    # Solicitante
    sol = (
        dff.get("nome_solicitante", pd.Series(dtype=str))
        .fillna("N/I")
        .astype(str)
        .str.strip()
        .replace({"": "N/I"})
        .value_counts()
        .reset_index()
    )
    sol.columns = ["Solicitante", "QTD"]
    top15_sol = sol.sort_values("QTD", ascending=False).head(15)
    fig_solicitante = px.bar(top15_sol, x="Solicitante", y="QTD", text="QTD")
    fig_solicitante.update_layout(title=None, xaxis_tickangle=-45)

    return fig_status, fig_impacto, fig_tecnico, fig_in1, fig_in2, fig_periodo, fig_solicitante

# =========================================================
# 5) BUILD FILTER OPTIONS (1x no boot)
# =========================================================
df0 = preparar_campos(get_data())

options_solicitante = opts_from_series(df0.get("nome_solicitante"))
options_mes = opts_from_series(df0.get("MES_EMISSAO"))
options_status = opts_from_series(df0.get("STATUS"))
options_tecnico = opts_from_series(df0.get("nm_tecAtual"))
options_input1 = opts_from_series(df0.get("input1"))
options_input2 = opts_from_series(df0.get("input2"))

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
                    html.Div("Solicitante", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_solicitante", options=options_solicitante, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Mês emissão", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_mes_emissao", options=options_mes, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Status", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_status", options=options_status, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Técnico", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_tecnico", options=options_tecnico, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Grupo", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_input1", options=options_input1, multi=True, placeholder="Selecione..."),
                    html.Hr(),

                    html.Div("Subgrupo", className="text-muted", style={"fontSize": "12px"}),
                    dcc.Dropdown(id="f_input2", options=options_input2, multi=True, placeholder="Selecione..."),
                ],
            ),
        ]
    ),
    className="shadow-sm",
)

main_content = html.Div(
    [
        html.H4("Dashboard (Template)", className="mb-2", style={"fontWeight": "700", "color": "#07242b", "textAlign": "center"}),

        dbc.Row(
            [
                dbc.Col(html.Div(id="kpi_total"), md=3),
                dbc.Col(html.Div(id="kpi_media"), md=3),
                dbc.Col(html.Div(id="kpi_abertos"), md=3),
                dbc.Col(html.Div(id="kpi_outro"), md=3),
            ],
            className="mt-2 g-3",
        ),

        dbc.Row(
            [
                dbc.Col(card_com_header("Chamados por Status", "g_status"), md=3),
                dbc.Col(card_com_header("Impacto", "g_impacto"), md=3),
                dbc.Col(card_com_header("Técnicos", "g_tecnico"), md=3),
                dbc.Col(card_com_header("Grupo", "g_input1"), md=3),
            ],
            className="mt-2 g-2",
        ),

        dbc.Row(
            [
                dbc.Col(card_com_header("Subgrupo", "g_input2"), md=4),
                dbc.Col(card_com_header("Quantidade por Período", "g_periodo"), md=4),
                dbc.Col(card_com_header("Top Solicitantes", "g_solicitante"), md=4),
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
    Output("kpi_total", "children"),
    Output("kpi_media", "children"),
    Output("kpi_abertos", "children"),
    Output("kpi_outro", "children"),
    Output("g_status", "figure"),
    Output("g_impacto", "figure"),
    Output("g_tecnico", "figure"),
    Output("g_input1", "figure"),
    Output("g_input2", "figure"),
    Output("g_periodo", "figure"),
    Output("g_solicitante", "figure"),
    Output("tbl", "data"),
    Output("tbl", "columns"),
    Input("interval_refresh", "n_intervals"),
    Input("f_solicitante", "value"),
    Input("f_mes_emissao", "value"),
    Input("f_status", "value"),
    Input("f_tecnico", "value"),
    Input("f_input1", "value"),
    Input("f_input2", "value"),
)
def update_all(n_intervals, f_solicitante, f_mes, f_status, f_tecnico, f_in1, f_in2):
    dff = preparar_campos(get_data())

    # filtros (padrão template)
    if f_solicitante:
        dff = dff[dff["nome_solicitante"].astype(str).isin([str(x) for x in f_solicitante])]
    if f_mes and "MES_EMISSAO" in dff.columns:
        dff = dff[dff["MES_EMISSAO"].astype(str).isin([str(x) for x in f_mes])]
    if f_status and "STATUS" in dff.columns:
        norm = [str(x).strip().upper() for x in (f_status if isinstance(f_status, list) else [f_status])]
        dff = dff[dff["STATUS"].astype(str).str.strip().str.upper().isin(norm)]
    if f_tecnico and "nm_tecAtual" in dff.columns:
        norm = [str(x).strip() for x in (f_tecnico if isinstance(f_tecnico, list) else [f_tecnico])]
        dff = dff[dff["nm_tecAtual"].astype(str).str.strip().isin(norm)]
    if f_in1 and "input1" in dff.columns:
        dff = dff[dff["input1"].astype(str).isin([str(x) for x in f_in1])]
    if f_in2 and "input2" in dff.columns:
        dff = dff[dff["input2"].astype(str).isin([str(x) for x in f_in2])]

    # KPIs (exemplo)
    total = len(dff)
    meses_distintos = int(dff["MES_EMISSAO"].dropna().nunique()) if "MES_EMISSAO" in dff.columns else 0
    media_mes = (total / meses_distintos) if meses_distintos > 0 else total
    abertos = int(dff["STATUS"].astype(str).str.upper().ne("FINALIZADO").sum()) if "STATUS" in dff.columns else 0

    k1 = kpi_card("Qtde Registros", f"{total:,}".replace(",", "."), icon="bi bi-collection")
    k2 = kpi_card("Média por Mês", br_num(media_mes, 0), f"Meses: {meses_distintos}", icon="bi bi-calendar3")
    k3 = kpi_card("Em aberto (exemplo)", f"{abertos:,}".replace(",", "."), icon="bi bi-exclamation-circle")
    k4 = kpi_card("Outro KPI", "—", icon="bi bi-graph-up-arrow")

    # figs
    fig_status, fig_impacto, fig_tecnico, fig_in1, fig_in2, fig_periodo, fig_solicitante = build_figures(dff)

    # tabela
    preferidas = [c for c in ["STATUS", "MES_EMISSAO", "nome_solicitante", "nm_tecAtual", "input1", "input2", "dt_emissao"] if c in dff.columns]
    cols = preferidas + [c for c in dff.columns if c not in preferidas]
    view = dff[cols].copy()

    data = view.to_dict("records")
    columns = [{"name": c, "id": c} for c in view.columns]

    return k1, k2, k3, k4, fig_status, fig_impacto, fig_tecnico, fig_in1, fig_in2, fig_periodo, fig_solicitante, data, columns

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
    app.run(debug=True, port=8055)
