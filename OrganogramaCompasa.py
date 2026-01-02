
import requests
import pandas as pd
import numpy as np
import plotly.express as px
from dash import Dash, dcc, html, dash_table, Input, Output, State

from dotenv import load_dotenv
load_dotenv()

# =========================================
# 0) PARAMETROS (ajuste aqui seus IDs)
# =========================================
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# ============================
# 1) Autenticação via Client Credentials
# ============================
url_token = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
scope = "https://graph.microsoft.com/.default"

data = {
    "client_id": CLIENT_ID,
    "scope": scope,
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials"
}

resp_token = requests.post(url_token, data=data)
dados_token = resp_token.json()
if "access_token" not in dados_token:
    raise Exception(f"Erro ao obter token: {dados_token}")

token = dados_token["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# ============================
# 2) Buscar todos os usuários
# ============================
users = []
url_users = (
    "https://graph.microsoft.com/v1.0/users?"
    "$top=999&$select=id,displayName,mail,jobTitle,department,officeLocation,accountEnabled"
)

resp = requests.get(url_users, headers=headers).json()
users = resp.get("value", [])

while "@odata.nextLink" in resp:
    resp = requests.get(resp["@odata.nextLink"], headers=headers).json()
    users.extend(resp["value"])

df_users = pd.DataFrame(users)

# Renomeia colunas para padrão interno
df_users = df_users.rename(columns={
    "displayName": "Nome",
    "mail": "Email",
    "jobTitle": "Funcao",
    "department": "Setor",
    "officeLocation": "Local"
})

# Mantém só usuários ativos (se coluna existir)
if "accountEnabled" in df_users.columns:
    df_users = df_users[df_users["accountEnabled"] == True]

# Filtro extra para tirar contas técnicas, deletadas etc.
df_users = df_users[
    df_users["Email"].notna() &
    df_users["Email"].str.contains("@", na=False) &
    ~df_users["Email"].str.contains("onmicrosoft", na=False) &
    ~df_users["Email"].str.contains("deleted", na=False)
]


# ============================
# 3) Buscar o gestor de cada usuário
# ============================
manager_list = []

for uid in df_users["id"]:
    url_manager = f"https://graph.microsoft.com/v1.0/users/{uid}/manager"
    r = requests.get(url_manager, headers=headers)

    if r.status_code == 200:
        manager = r.json()
        manager_list.append({
            "id": uid,
            "manager_id": manager.get("id"),
            "Gestor": manager.get("displayName")
        })
    else:
        manager_list.append({
            "id": uid,
            "manager_id": None,
            "Gestor": None
        })

df_manager = pd.DataFrame(manager_list)

# Une usuários + gestor
df_org = df_users.merge(df_manager, on="id", how="left")

# Reforça filtro de e-mail válido
df_org = df_org[
    df_org["Email"].notna() &
    df_org["Email"].str.contains("@", na=False) &
    ~df_org["Email"].str.contains("deleted", na=False) &
    ~df_org["Email"].str.contains("onmicrosoft", na=False)
]

df_org = df_org[df_org['Setor'].notna()]

# Guardar como "hierarquia base" (vamos usar direto)
df_hierarquia_base = df_org.copy()

# ============================
# 4) Função para montar o Sunburst via labels/parents
#    Empresa -> Gestores -> Subordinados (profundidade livre)
#    Em todos os níveis: Nome + Setor
# ============================
def build_sunburst(df_base):
    empresa = "Compasa do Brasil"

    # Pega pessoas únicas no recorte atual
    dfp = (
        df_base[["Nome", "Gestor", "Setor", "Funcao", "Local", "Email"]]
        .drop_duplicates(subset=["Nome"])
    )

    # mapa Nome -> (Setor, Função, Local, Email, Gestor)
    pessoas = {
        row["Nome"]: {
            "Setor": row["Setor"],
            "Funcao": row["Funcao"],
            "Local": row["Local"],
            "Email": row["Email"],
            "Gestor": row["Gestor"],
        }
        for _, row in dfp.iterrows()
    }

    # Conjunto de todos os nomes (nós) e seus gestores
    nomes = set(pessoas.keys())
    gestores = set(v["Gestor"] for v in pessoas.values() if pd.notna(v["Gestor"]))
    todos = nomes.union(gestores)

    # Cria DF de nós (empresa + todos os nomes/gestores)
    nodes = []

    # Nó raiz
    nodes.append({
        "id": empresa,
        "label": empresa,
        "parent": "",
        "Nome": empresa,
        "Gestor": "",
        "Setor": "",
        "Funcao": "",
        "Local": "",
        "Email": "",
        "Peso": 1,
    })

    for nome in todos:
        if pd.isna(nome):
            continue

        info = pessoas.get(nome, {})
        setor = info.get("Setor", "")
        funcao = info.get("Funcao", "")
        local = info.get("Local", "")
        email = info.get("Email", "")
        gestor = info.get("Gestor", None)

        # Label: Nome + Setor em todas as "fatias"
        label = f"{nome}<br>{setor if setor is not None else ''}"

        # Se o gestor existir dentro do conjunto, parent é o gestor; senão, é a empresa
        if pd.notna(gestor) and gestor in todos:
            parent = gestor
        else:
            parent = empresa

        nodes.append({
            "id": nome,
            "label": label,
            "parent": parent,
            "Nome": nome,
            "Gestor": gestor,
            "Setor": setor,
            "Funcao": funcao,
            "Local": local,
            "Email": email,
            "Peso": 1,
        })

    df_nodes = pd.DataFrame(nodes)

    fig = px.sunburst(
        df_nodes,
        ids="id",
        names="label",
        parents="parent",
        values="Peso",
        color="parent",  # cores por "chefe" imediato; pode trocar por 'Setor' se quiser
        hover_data={
            "Nome": True,
            "Gestor": True,
            "Funcao": True,
            "Setor": True,
            "Local": True,
            "Email": True
        }
    )

    # Mostra até X anéis no início, o resto aparece ao clicar (drilldown)
    fig.update_traces(maxdepth=4)

    fig.update_layout(
        title="Organograma (Sunburst) - Nome + Departamento em todos os níveis",
        margin=dict(t=50, l=10, r=10, b=10)
    )

    return fig

# ============================
# 5) DataFrame de Problemas (inclui Local)
# ============================
df_pessoas = df_hierarquia_base.copy()
problemas = []

# 1) Pessoas sem gestor
masc_sem_gestor = df_pessoas["Gestor"].isna() | (df_pessoas["Gestor"] == "")
for _, row in df_pessoas[masc_sem_gestor].iterrows():
    problemas.append({
        "Nome": row["Nome"],
        "Email": row.get("Email"),
        "Setor": row.get("Setor"),
        "Funcao": row.get("Funcao"),
        "Local": row.get("Local"),
        "Gestor": row.get("Gestor"),
        "TipoProblema": "Sem gestor definido",
        "Detalhe": "Colaborador não possui gestor configurado no Teams/Entra."
    })

# 2) Pessoas sem setor
masc_sem_setor = df_pessoas["Setor"].isna() | (df_pessoas["Setor"] == "")
for _, row in df_pessoas[masc_sem_setor].iterrows():
    problemas.append({
        "Nome": row["Nome"],
        "Email": row.get("Email"),
        "Setor": row.get("Setor"),
        "Funcao": row.get("Funcao"),
        "Local": row.get("Local"),
        "Gestor": row.get("Gestor"),
        "TipoProblema": "Sem setor",
        "Detalhe": "Campo 'department' (Setor) não está preenchido no AD."
    })

# 3) Pessoas sem função
masc_sem_funcao = df_pessoas["Funcao"].isna() | (df_pessoas["Funcao"] == "")
for _, row in df_pessoas[masc_sem_funcao].iterrows():
    problemas.append({
        "Nome": row["Nome"],
        "Email": row.get("Email"),
        "Setor": row.get("Setor"),
        "Funcao": row.get("Funcao"),
        "Local": row.get("Local"),
        "Gestor": row.get("Gestor"),
        "TipoProblema": "Sem função",
        "Detalhe": "Campo 'jobTitle' (Função) não está preenchido no AD."
    })

# 4) Gestores (pelo cargo) sem subordinados
masc_eh_gestor_cargo = df_pessoas["Funcao"].str.contains(
    "gerente|coordenador|diretor|supervisor",
    case=False,
    na=False
)
df_gestores_teoricos = df_pessoas[masc_eh_gestor_cargo]

for _, row in df_gestores_teoricos.iterrows():
    nome_gestor = row["Nome"]
    qtd_subordinados = (df_pessoas["Gestor"] == nome_gestor).sum()

    if qtd_subordinados == 0:
        problemas.append({
            "Nome": row["Nome"],
            "Email": row.get("Email"),
            "Setor": row.get("Setor"),
            "Funcao": row.get("Funcao"),
            "Local": row.get("Local"),
            "Gestor": row.get("Gestor"),
            "TipoProblema": "Gestor sem subordinados",
            "Detalhe": "Cargo indica liderança, mas não há ninguém apontando para ele como gestor."
        })

df_problemas = pd.DataFrame(problemas)
if not df_problemas.empty:
    df_problemas = df_problemas.sort_values(["TipoProblema", "Setor", "Nome"])


df_problemas_base = df_problemas.copy()


# ============================
# 6) Opções dos dropdowns
# ============================
setores_unicos = sorted(
    s for s in df_hierarquia_base["Setor"].dropna().unique()
)
dropdown_setor_options = [{"label": "Todos os Setores", "value": "TODOS"}] + [
    {"label": s, "value": s} for s in setores_unicos
]

nomes_unicos = sorted(
    n for n in df_hierarquia_base["Nome"].dropna().unique()
)
dropdown_nome_options = [{"label": "Todos os Nomes", "value": "TODOS"}] + [
    {"label": n, "value": n} for n in nomes_unicos
]

# ============================
# 7) APP DASH
# ============================
app = Dash(__name__)

app.layout = html.Div(
    style={"fontFamily": "Arial", "margin": "20px"},
    children=[
        html.H1("Gestão de Organograma do Teams", style={"textAlign": "center"}),

        html.Div(
            style={
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "marginBottom": "10px"
            },
            children=[
                html.Div(
                    children=[
                        html.Label("Filtrar por Setor:"),
                        dcc.Dropdown(
                            id="filtro-setor",
                            options=dropdown_setor_options,
                            value="TODOS",
                            clearable=False,
                            style={"width": "300px", "marginBottom": "8px"}
                        ),
                        html.Label("Filtrar por Nome:"),
                        dcc.Dropdown(
                            id="filtro-nome",
                            options=dropdown_nome_options,
                            value="TODOS",
                            clearable=False,
                            style={"width": "300px"}
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.Button(
                            "Expandir gráfico",
                            id="btn-expandir-grafico",
                            n_clicks=0,
                            style={"padding": "8px 16px", "marginRight": "10px"}
                        ),
                        html.Button(
                            "Exportar problemas (Excel)",
                            id="btn-exportar",
                            n_clicks=0,
                            style={"padding": "8px 16px"}
                        ),
                        dcc.Download(id="download-problemas")
                    ]
                )
            ]
        ),

        html.Div(
            id="linha-principal",
            style={"display": "flex", "flexDirection": "row", "gap": "20px"},
            children=[
                html.Div(
                    id="col-grafico",
                    style={"flex": "1", "minWidth": "400px"},
                    children=[
                        html.H3("Organograma (Sunburst)"),
                        dcc.Graph(
                            id="grafico-organograma",
                            figure=build_sunburst(df_hierarquia_base),
                            style={"height": "600px"}
                        )
                    ]
                ),
                html.Div(
                    id="col-tabela",
                    style={"flex": "1", "minWidth": "400px"},
                    children=[
                        html.H3("Problemas de Cadastro / Organograma"),
                        dash_table.DataTable(
                            id="tabela-problemas",
                            columns=[{"name": c, "id": c} for c in df_problemas_base.columns],
                            data=df_problemas_base.to_dict("records"),
                            page_size=15,
                            filter_action="native",
                            sort_action="native",
                            sort_mode="multi",
                            style_table={"overflowX": "auto", "maxHeight": "600px"},
                            style_cell={
                                "fontFamily": "Arial",
                                "fontSize": 12,
                                "whiteSpace": "normal",
                                "height": "auto",
                                "textAlign": "left"
                            },
                            style_header={
                                "backgroundColor": "#f0f0f0",
                                "fontWeight": "bold"
                            }
                        )
                    ]
                )
            ]
        )
    ]
)

# ============================
# 8) CALLBACK – filtros (Setor + Nome)
# ============================
@app.callback(
    Output("grafico-organograma", "figure"),
    Output("tabela-problemas", "data"),
    Input("filtro-setor", "value"),
    Input("filtro-nome", "value")
)
def atualizar_por_filtros(setor, nome):
    df_h = df_hierarquia_base.copy()
    df_p = df_problemas_base.copy()

    if setor is not None and setor != "TODOS":
        df_h = df_h[df_h["Setor"] == setor]
        df_p = df_p[df_p["Setor"] == setor]

    if nome is not None and nome != "TODOS":
        df_h = df_h[df_h["Nome"] == nome]
        df_p = df_p[df_p["Nome"] == nome]

    fig = build_sunburst(df_h)
    dados_tabela = df_p.to_dict("records")
    return fig, dados_tabela

# ============================
# 9) CALLBACK – expandir/voltar gráfico
# ============================
@app.callback(
    Output("col-grafico", "style"),
    Output("col-tabela", "style"),
    Output("grafico-organograma", "style"),
    Output("btn-expandir-grafico", "children"),
    Input("btn-expandir-grafico", "n_clicks"),
    prevent_initial_call=False
)
def toggle_expandir_grafico(n_clicks):
    if n_clicks is None or n_clicks % 2 == 0:
        style_col_grafico = {"flex": "1", "minWidth": "400px"}
        style_col_tabela = {"flex": "1", "minWidth": "400px"}
        style_grafico = {"height": "600px"}
        texto_botao = "Expandir gráfico"
    else:
        style_col_grafico = {"flex": "1", "minWidth": "400px", "width": "100%"}
        style_col_tabela = {"display": "none"}
        style_grafico = {"height": "950px"}
        texto_botao = "Voltar visão lado a lado"

    return style_col_grafico, style_col_tabela, style_grafico, texto_botao

# ============================
# 10) CALLBACK – exportar Excel
# ============================
@app.callback(
    Output("download-problemas", "data"),
    Input("btn-exportar", "n_clicks"),
    State("filtro-setor", "value"),
    State("filtro-nome", "value"),
    prevent_initial_call=True
)
def exportar_problemas(n_clicks, setor, nome):
    df_exp = df_problemas_base.copy()

    if setor is not None and setor != "TODOS":
        df_exp = df_exp[df_exp["Setor"] == setor]

    if nome is not None and nome != "TODOS":
        df_exp = df_exp[df_exp["Nome"] == nome]

    return dcc.send_data_frame(
        df_exp.to_excel,
        "problemas_organograma_teams.xlsx",
        index=False
    )
if __name__ == "__main__":
    app.run(debug=True, port=8052)