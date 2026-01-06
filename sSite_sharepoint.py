import io
from datetime import datetime, timedelta

import pandas as pd
import requests
from azure.identity import ClientSecretCredential

from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.io as pio
# import dash_daq as daq

import plotly.graph_objects as go

from dotenv import load_dotenv
load_dotenv()
import os

# Tema dark para os gráficos
pio.templates.default = "plotly_dark"

# ====================================================
# CONFIGURAÇÃO DO APP (Entra ID / Azure AD)
# ====================================================

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
)

GRAPH_BASE = "https://graph.microsoft.com/v1.0/"


# ====================================================
# HELPERS GERAIS PARA O GRAPH
# ====================================================

def metric_storage_total(period: str = "D30") -> dict:
    """
    Usa o report getSharePointSiteUsageStorage para pegar 'used' (real),
    e usa SPO_TOTAL_TB (env) como capacidade total do tenant.
    """
    df = download_graph_report(
        f"reports/getSharePointSiteUsageStorage(period='{period}')"
    )

    if df.empty or "Storage Used (Byte)" not in df.columns:
        return {"percent_used": None, "used_tb": None, "allocated_tb": None}

    used_bytes = float(df["Storage Used (Byte)"].iloc[-1])
    used_tb = used_bytes / 1024**4

    allocated_tb = os.getenv("SPO_TOTAL_TB")
    allocated_tb = float(allocated_tb) if allocated_tb else None

    if not allocated_tb or allocated_tb <= 0:
        return {"percent_used": None, "used_tb": used_tb, "allocated_tb": None}

    percent_used = (used_tb / allocated_tb) * 100
    return {"percent_used": float(percent_used), "used_tb": float(used_tb), "allocated_tb": float(allocated_tb)}


def get_token_header() -> dict:
    token = credential.get_token("https://graph.microsoft.com/.default").token
    return {"Authorization": f"Bearer {token}"}


def call_graph(path: str) -> dict:
    """
    Chamada simples ao Graph para JSON.
    Aceita path relativo (ex: 'users?...') ou URL completa (https://graph...).
    """
    headers = get_token_header()
    if path.startswith("http"):
        url = path
    else:
        url = GRAPH_BASE + path

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def download_graph_report(relative_url: str) -> pd.DataFrame:
    """
    Relatórios CSV (SharePoint / OneDrive / etc):
      1) chama /reports/... (vem 302 com Location)
      2) baixa o CSV da URL em Location
    """
    headers = get_token_header()
    resp = requests.get(GRAPH_BASE + relative_url, headers=headers, allow_redirects=False)
    resp.raise_for_status()

    if "Location" not in resp.headers:
        raise RuntimeError(
            f"Resposta sem Location, status={resp.status_code}, body={resp.text}"
        )

    csv_url = resp.headers["Location"]
    csv_resp = requests.get(csv_url)
    csv_resp.raise_for_status()

    df = pd.read_csv(io.StringIO(csv_resp.text))
    return df


def get_token() -> str:
    token = credential.get_token("https://graph.microsoft.com/.default")
    return token.token


def graph_get(endpoint: str):
    """
    Faz GET no Microsoft Graph para endpoints simples que retornam 'value'.
    Exemplo: /subscribedSkus
    """
    headers = {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json",
    }
    url = f"https://graph.microsoft.com/v1.0{endpoint}"

    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    else:
        print("Erro na API:", resp.text)
        return []


# ====================================================
# LICENÇAS CONTRATADAS E USUÁRIOS POR LICENÇA
# ====================================================

def df_licencas_contratadas() -> pd.DataFrame:
    rows = graph_get("/subscribedSkus")
    if not rows:
        return pd.DataFrame(
            columns=["skuId", "skuPartNumber", "capacidade", "consumido", "sobra"]
        )

    data = [
        {
            "skuId": r.get("skuId"),
            "skuPartNumber": r.get("skuPartNumber"),
            "capacidade": r.get("prepaidUnits", {}).get("enabled", 0),
            "consumido": r.get("consumedUnits", 0),
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    df["sobra"] = df["capacidade"] - df["consumido"]
    return df


def df_usuarios_licencas(top: int = 999) -> pd.DataFrame:
    """
    Retorna um DataFrame com uma linha por usuário e por SKU atribuído.
    Colunas: userId, displayName, userPrincipalName, skuId
    Usa call_graph para lidar com @odata.nextLink.
    """
    users = []
    url = "users?$select=id,displayName,userPrincipalName,assignedLicenses&$top=999"

    while url:
        data = call_graph(url)
        valores = data.get("value", [])
        users.extend(valores)
        url = data.get("@odata.nextLink")

    if not users:
        return pd.DataFrame(
            columns=["userId", "displayName", "userPrincipalName", "skuId"]
        )

    registros = []
    for u in users:
        uid = u.get("id")
        name = u.get("displayName")
        upn = u.get("userPrincipalName")
        assigned_lic = u.get("assignedLicenses", [])

        for lic in assigned_lic:
            registros.append(
                {
                    "userId": uid,
                    "displayName": name,
                    "userPrincipalName": upn,
                    "skuId": lic.get("skuId"),
                }
            )

    if not registros:
        return pd.DataFrame(
            columns=["userId", "displayName", "userPrincipalName", "skuId"]
        )

    df = pd.DataFrame(registros)
    return df


# ====================================================
# 1) MÉTRICAS DE SHAREPOINT (GOVERNANÇA)
# ====================================================

def metric_storage_trend(period: str = "D30") -> pd.DataFrame:
    df = download_graph_report(
        f"reports/getSharePointSiteUsageStorage(period='{period}')"
    )
    df["Report Date"] = pd.to_datetime(df["Report Date"])
    df["Storage Used (GB)"] = df["Storage Used (Byte)"] / 1024**3
    df = df.sort_values("Report Date")
    return df[["Report Date", "Storage Used (GB)", "Report Period"]]


def metric_site_storage_usage(period: str = "D30") -> pd.DataFrame:
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    df["Storage Used (GB)"] = df["Storage Used (Byte)"] / (1024**3)
    df["Storage Allocated (GB)"] = df["Storage Allocated (Byte)"] / (1024**3)

    return df[
        [
            "Site URL",
            "Owner Display Name",
            "Storage Used (GB)",
            "Storage Allocated (GB)",
            "File Count",
            "Last Activity Date",
        ]
    ].sort_values("Storage Used (GB)", ascending=False)


def metric_active_vs_inactive(period: str = "D30") -> pd.DataFrame:
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    df["Last Activity Date"] = pd.to_datetime(
        df["Last Activity Date"], errors="coerce"
    )
    df["Report Refresh Date"] = pd.to_datetime(df["Report Refresh Date"])

    df["Dias_Inatividade"] = (
        df["Report Refresh Date"] - df["Last Activity Date"]
    ).dt.days
    df["Status"] = df["Dias_Inatividade"].apply(
        lambda d: "Ativo" if d <= 15 else "Inativo"
    )

    resumo = df.groupby("Status").size().reset_index(name="Qtd Sites")
    return resumo


def metric_top_sites_by_storage(period: str = "D30", top_n: int = 10) -> pd.DataFrame:
    df = metric_site_storage_usage(period)
    return df.sort_values("Storage Used (GB)", ascending=False).head(top_n)


def metric_top_sites_by_page_views(period: str = "D30", top_n: int = 10) -> pd.DataFrame:
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    if "Page View Count" not in df.columns:
        df["Page View Count"] = 0

    df = df[["Site URL", "Owner Display Name", "Page View Count"]]
    return df.sort_values("Page View Count", ascending=False).head(top_n)


def metric_inactive_sites(
    period: str = "D180", inactive_days: int = 90
) -> pd.DataFrame:
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    df["Last Activity Date"] = pd.to_datetime(
        df["Last Activity Date"], errors="coerce"
    )
    df["Report Refresh Date"] = pd.to_datetime(df["Report Refresh Date"])

    df["Dias_Inatividade"] = (
        df["Report Refresh Date"] - df["Last Activity Date"]
    ).dt.days
    return df[df["Dias_Inatividade"] >= inactive_days].sort_values(
        "Dias_Inatividade", ascending=False
    )


def metric_file_counts(period: str = "D30") -> pd.DataFrame:
    df = download_graph_report(
        f"reports/getSharePointSiteUsageFileCounts(period='{period}')"
    )

    df["Report Date"] = pd.to_datetime(df["Report Date"])
    df = df.sort_values("Report Date")

    total_col = "Total" if "Total" in df.columns else "File Count"
    active_col = "Active" if "Active" in df.columns else "Active File Count"

    df["% Ativos"] = (df[active_col] / df[total_col]) * 100

    df_out = df[["Report Date", total_col, active_col, "% Ativos"]].rename(
        columns={
            total_col: "Total Arquivos",
            active_col: "Arquivos Ativos",
        }
    )
    return df_out


def metric_top_active_users(period: str = "D30", top_n: int = 20) -> pd.DataFrame:
    df = download_graph_report(
        f"reports/getSharePointActivityUserDetail(period='{period}')"
    )

    df["Total Actions"] = (
        df.get("Viewed Or Edited File Count", 0)
        + df.get("Viewed or Edited File Count", 0)
        + df.get("Synced File Count", 0)
        + df.get("Shared Internally Count", 0)
        + df.get("Shared Internally File Count", 0)
        + df.get("Shared Externally Count", 0)
        + df.get("Shared Externally File Count", 0)
    )

    return df[["User Principal Name", "Total Actions"]].sort_values(
        "Total Actions", ascending=False
    ).head(top_n)


def metric_active_user_trend(period: str = "D30") -> pd.DataFrame:
    rel_url = f"reports/getSharePointActivityUserCounts(period='{period}')"
    df = download_graph_report(rel_url)

    if "Report Date" in df.columns:
        df["Report Date"] = pd.to_datetime(df["Report Date"])
        df = df.sort_values("Report Date")

    candidates = [
        "Active Users",
        "Active User Count",
        "Active",
        "Total Active Users",
    ]
    active_col = next((c for c in candidates if c in df.columns), None)

    cols_out = []
    if "Report Date" in df.columns:
        cols_out.append("Report Date")
    if active_col:
        cols_out.append(active_col)
    if "Report Period" in df.columns:
        cols_out.append("Report Period")

    if not cols_out:
        return df

    df_out = df[cols_out].copy()
    if active_col:
        df_out = df_out.rename(columns={active_col: "Active Users"})

    return df_out


def metric_inactive_users(
    period: str = "D180", inactive_days: int = 30
) -> pd.DataFrame:
    """
    Usuários do SharePoint inativos há 'inactive_days' dias ou mais,
    considerando apenas usuários NÃO deletados (Is Deleted == False).
    """
    rel_url = f"reports/getSharePointActivityUserDetail(period='{period}')"
    df = download_graph_report(rel_url)

    user_col_candidates = [
        "User Principal Name",
        "UserPrincipalName",
        "UPN",
    ]
    display_name_candidates = [
        "User Display Name",
        "Display Name",
    ]

    user_col = next((c for c in user_col_candidates if c in df.columns), None)
    display_col = next((c for c in display_name_candidates if c in df.columns), None)

    if not user_col or "Last Activity Date" not in df.columns:
        return pd.DataFrame()

    if "Is Deleted" in df.columns:
        df = df[df["Is Deleted"] == False].copy()

    df["Last Activity Date"] = pd.to_datetime(
        df["Last Activity Date"], errors="coerce"
    )

    if "Report Refresh Date" in df.columns:
        df["Report Refresh Date"] = pd.to_datetime(
            df["Report Refresh Date"], errors="coerce"
        )
        data_referencia = df["Report Refresh Date"].max()
    else:
        data_referencia = pd.to_datetime(datetime.utcnow().date())

    df["Dias_Inatividade"] = (
        data_referencia - df["Last Activity Date"]
    ).dt.days

    df["Motivo_Inatividade"] = df["Dias_Inatividade"].apply(
        lambda d: (
            "Nenhuma atividade registrada no período do relatório"
            if pd.isna(d) or d < 0
            else f"Sem atividade há {int(d)} dias"
        )
    )

    df_inativos = df[df["Dias_Inatividade"] >= inactive_days].copy()
    df_inativos = df_inativos.sort_values(
        "Dias_Inatividade", ascending=False
    )

    cols_saida = [user_col]
    if display_col:
        cols_saida.append(display_col)
    cols_saida += ["Last Activity Date", "Dias_Inatividade", "Motivo_Inatividade"]

    df_out = df_inativos[cols_saida].rename(
        columns={
            user_col: "Usuario",
            display_col: "Nome" if display_col else display_col,
        }
    )

    df_out["Data_Referencia"] = data_referencia
    df_out["Criterio_Inatividade"] = (
        f"Usuário não deletado e sem atividade no SharePoint há >= {inactive_days} dias "
        f"(Data_Referencia vs Last Activity Date)."
    )

    return df_out


def metric_sites_near_quota(
    period: str = "D30", limiar_percentual: float = 80.0
) -> pd.DataFrame:
    """
    Sites com uso de storage acima de 'limiar_percentual' da quota.
    """
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    if "Storage Used (Byte)" not in df.columns or "Storage Allocated (Byte)" not in df.columns:
        return pd.DataFrame()

    df["Storage Used (GB)"] = df["Storage Used (Byte)"] / 1024**3
    df["Storage Allocated (GB)"] = df["Storage Allocated (Byte)"] / 1024**3
    df["Uso_%"] = (
        df["Storage Used (Byte)"] / df["Storage Allocated (Byte)"]
    ) * 100

    df_alerta = df[df["Uso_%"] >= limiar_percentual].copy()
    df_alerta = df_alerta.sort_values("Uso_%", ascending=False)

    cols = [
        "Site URL",
        "Owner Display Name",
        "Storage Used (GB)",
        "Storage Allocated (GB)",
        "Uso_%",
        "Last Activity Date",
    ]
    cols_existentes = [c for c in cols if c in df_alerta.columns]

    return df_alerta[cols_existentes]


def metric_sites_with_external_sharing(
    period: str = "D30", min_external_shares: int = 1
) -> pd.DataFrame:
    """
    Sites com compartilhamento externo no período.
    """
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    candidates = [
        "Shared Externally File Count",
        "Shared Externally",
        "External Sharing",
    ]
    external_col = next((c for c in candidates if c in df.columns), None)

    if external_col is None:
        return pd.DataFrame()

    df_ext = df[df[external_col] >= min_external_shares].copy()
    df_ext = df_ext.sort_values(external_col, ascending=False)

    cols = [
        "Site URL",
        "Owner Display Name",
        external_col,
        "Last Activity Date",
        "File Count",
    ]
    cols_existentes = [c for c in cols if c in df_ext.columns]

    return df_ext[cols_existentes].rename(
        columns={external_col: "Compart_Externos"}
    )


def metric_sites_without_owner(period: str = "D30") -> pd.DataFrame:
    """
    Sites sem Owner Display Name no relatório de uso do SharePoint.
    """
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    if "Owner Display Name" not in df.columns:
        return pd.DataFrame()

    df_sem_owner = df[
        df["Owner Display Name"].isna()
        | (df["Owner Display Name"] == "")
    ].copy()

    if df_sem_owner.empty:
        return df_sem_owner

    cols = [
        "Site URL",
        "Owner Display Name",
        "Storage Used (Byte)",
        "File Count",
        "Last Activity Date",
    ]
    cols_existentes = [c for c in cols if c in df_sem_owner.columns]

    return df_sem_owner[cols_existentes]


def metric_orphan_sites(
    period: str = "D180", inactive_days: int = 180
) -> pd.DataFrame:
    """
    Sites 'órfãos' = sem owner (no relatório) E inativos há >= inactive_days.
    """
    df = download_graph_report(
        f"reports/getSharePointSiteUsageDetail(period='{period}')"
    )

    if "Last Activity Date" not in df.columns or "Report Refresh Date" not in df.columns:
        return pd.DataFrame()

    df["Last Activity Date"] = pd.to_datetime(
        df["Last Activity Date"], errors="coerce"
    )
    df["Report Refresh Date"] = pd.to_datetime(
        df["Report Refresh Date"], errors="coerce"
    )

    df["Dias_Inatividade"] = (
        df["Report Refresh Date"] - df["Last Activity Date"]
    ).dt.days

    if "Owner Display Name" in df.columns:
        sem_owner = df["Owner Display Name"].isna() | (
            df["Owner Display Name"] == ""
        )
    else:
        sem_owner = pd.Series([True] * len(df), index=df.index)

    orfaos = df[sem_owner & (df["Dias_Inatividade"] >= inactive_days)].copy()
    orfaos = orfaos.sort_values("Dias_Inatividade", ascending=False)

    cols = [
        "Site URL",
        "Owner Display Name",
        "Dias_Inatividade",
        "Last Activity Date",
        "Storage Used (Byte)",
        "File Count",
    ]
    cols_existentes = [c for c in cols if c in orfaos.columns]

    return orfaos[cols_existentes]


# ====================================================
# 2) INVENTÁRIO DE USUÁRIOS / LICENÇAS
# ====================================================

def get_all_users_with_licenses() -> pd.DataFrame:
    users = []
    url = "users?$select=displayName,userPrincipalName,accountEnabled,assignedLicenses&$top=999"

    while url:
        data = call_graph(url)
        users.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    if not users:
        return pd.DataFrame(
            columns=[
                "Usuario",
                "Nome",
                "ContaHabilitada",
                "Bloqueado",
                "Qtd_Licencas",
                "Consome_Licenca",
            ]
        )

    df = pd.json_normalize(users)

    df = df.rename(
        columns={
            "displayName": "Nome",
            "userPrincipalName": "Usuario",
            "accountEnabled": "ContaHabilitada",
        }
    )

    def count_lic(x):
        if isinstance(x, list):
            return len(x)
        return 0

    df["Qtd_Licencas"] = df["assignedLicenses"].apply(count_lic)
    df["Consome_Licenca"] = df["Qtd_Licencas"] > 0
    df["Bloqueado"] = ~df["ContaHabilitada"]

    return df[
        [
            "Usuario",
            "Nome",
            "ContaHabilitada",
            "Bloqueado",
            "Qtd_Licencas",
            "Consome_Licenca",
        ]
    ]


def get_sharepoint_activity_users(period: str = "D180") -> pd.DataFrame:
    rel_url = f"reports/getSharePointActivityUserDetail(period='{period}')"
    df = download_graph_report(rel_url)

    upn_candidates = ["User Principal Name", "UserPrincipalName", "UPN"]
    upn_col = next((c for c in upn_candidates if c in df.columns), None)
    if not upn_col:
        return pd.DataFrame(
            columns=["Usuario", "LastActivityDate", "IsDeletedRelatorio"]
        )

    df["Last Activity Date"] = pd.to_datetime(
        df["Last Activity Date"], errors="coerce"
    )
    if "Is Deleted" in df.columns:
        df["IsDeletedRelatorio"] = df["Is Deleted"]
    else:
        df["IsDeletedRelatorio"] = False

    df_out = df[[upn_col, "Last Activity Date", "IsDeletedRelatorio"]].rename(
        columns={
            upn_col: "Usuario",
            "Last Activity Date": "LastActivityDate",
        }
    )

    return df_out

def get_teams_activity_users(period: str = "D180") -> pd.DataFrame:
    """
    Última atividade no Teams por usuário (UPN).
    Retorna colunas: Usuario, TeamsLastActivityDate
    """
    rel_url = f"reports/getTeamsUserActivityUserDetail(period='{period}')"
    df = download_graph_report(rel_url)

    upn_candidates = ["User Principal Name", "UserPrincipalName", "UPN"]
    upn_col = next((c for c in upn_candidates if c in df.columns), None)

    if not upn_col or "Last Activity Date" not in df.columns:
        return pd.DataFrame(columns=["Usuario", "TeamsLastActivityDate"])

    # Converte data
    df["Last Activity Date"] = pd.to_datetime(df["Last Activity Date"], errors="coerce")

    # Remove deletados (se existir)
    if "Is Deleted" in df.columns:
        df = df[df["Is Deleted"] == False].copy()

    df_out = (
        df[[upn_col, "Last Activity Date"]]
        .rename(columns={upn_col: "Usuario", "Last Activity Date": "TeamsLastActivityDate"})
        .dropna(subset=["Usuario"])
    )

    # Se tiver duplicado, mantém o mais recente
    df_out = df_out.sort_values("TeamsLastActivityDate").drop_duplicates("Usuario", keep="last")

    return df_out

def build_user_inventory(period: str = "D180", inactive_days: int = 30) -> pd.DataFrame:
    df_users = get_all_users_with_licenses()
    df_sp = get_sharepoint_activity_users(period)
    df_teams = get_teams_activity_users(period)

    df = df_users.merge(df_sp, on="Usuario", how="left")
    df = df.merge(df_teams, on="Usuario", how="left")

    hoje = pd.to_datetime(datetime.utcnow().date())

    # SharePoint (já existia)
    df["Dias_Inatividade"] = (hoje - df["LastActivityDate"]).dt.days
    df["Nunca_Acessou_SP"] = df["LastActivityDate"].isna()

    df["Inativo_SP"] = (
        df["Consome_Licenca"]
        & df["ContaHabilitada"]
        & (df["Dias_Inatividade"] >= inactive_days)
    )

    df["Ativo_SP"] = (
        df["Consome_Licenca"]
        & df["ContaHabilitada"]
        & (df["Dias_Inatividade"] < inactive_days)
        & (~df["Nunca_Acessou_SP"])
    )

    df["Candidato_Remover_Licenca"] = df["Consome_Licenca"] & (
        df["Nunca_Acessou_SP"] | (df["Dias_Inatividade"] >= inactive_days)
    )

    # Teams (NOVO)
    df["TeamsDias_Inatividade"] = (hoje - df["TeamsLastActivityDate"]).dt.days
    df.loc[df["TeamsLastActivityDate"].isna(), "TeamsDias_Inatividade"] = pd.NA

    return df


def view_usuarios_ativos(df_inventory: pd.DataFrame) -> pd.DataFrame:
    return df_inventory[df_inventory["Ativo_SP"]][
        ["Usuario", "Nome", "Dias_Inatividade", "LastActivityDate"]
    ]


def view_usuarios_inativos(df_inventory: pd.DataFrame) -> pd.DataFrame:
    return df_inventory[df_inventory["Inativo_SP"]][
        ["Usuario", "Nome", "Dias_Inatividade", "LastActivityDate"]
    ]


def view_usuarios_bloqueados(df_inventory: pd.DataFrame) -> pd.DataFrame:
    return df_inventory[df_inventory["Bloqueado"]][
        ["Usuario", "Nome", "Bloqueado", "Qtd_Licencas", "Consome_Licenca"]
    ]


def view_quem_consome_licenca(df_inventory: pd.DataFrame) -> pd.DataFrame:
    return df_inventory[df_inventory["Consome_Licenca"]][
        ["Usuario", "Nome", "Qtd_Licencas", "Bloqueado", "Dias_Inatividade"]
    ]


def view_quem_sem_licenca(df_inventory: pd.DataFrame) -> pd.DataFrame:
    return df_inventory[~df_inventory["Consome_Licenca"]][
        ["Usuario", "Nome", "Bloqueado"]
    ]


def view_quem_nunca_acessou(df_inventory: pd.DataFrame) -> pd.DataFrame:
    return df_inventory[
        df_inventory["Nunca_Acessou_SP"] & df_inventory["Consome_Licenca"]
    ][["Usuario", "Nome", "Qtd_Licencas"]]


def view_quem_deveria_perder_licenca(df_inventory: pd.DataFrame) -> pd.DataFrame:
    return df_inventory[df_inventory["Candidato_Remover_Licenca"]][
        [
            "Usuario",
            "Nome",
            "Qtd_Licencas",
            "Bloqueado",
            "Nunca_Acessou_SP",
            "Dias_Inatividade",
            "LastActivityDate",
        ]
    ]


def view_usuarios_bloqueados_consumindo_licenca(
    df_inventory: pd.DataFrame,
) -> pd.DataFrame:
    filtro = df_inventory["Bloqueado"] & df_inventory["Consome_Licenca"]
    return df_inventory[filtro][
        [
            "Usuario",
            "Nome",
            "Qtd_Licencas",
            "Bloqueado",
            "ContaHabilitada",
            "Dias_Inatividade",
            "LastActivityDate",
        ]
    ].sort_values("Qtd_Licencas", ascending=False)


# ====================================================
# PRÉ-CÁLCULOS PARA O DASH (rodam 1x na subida do app)
# ====================================================

df_skus = df_licencas_contratadas()
df_users_lic = df_usuarios_licencas()

# garantir skuPartNumber em df_users_lic
if not df_users_lic.empty and not df_skus.empty:
    df_users_lic = df_users_lic.merge(
        df_skus[["skuId", "skuPartNumber"]], on="skuId", how="left"
    )
else:
    df_users_lic = pd.DataFrame(
        columns=[
            "userId",
            "displayName",
            "userPrincipalName",
            "skuId",
            "skuPartNumber",
        ]
    )

# Resumo por SKU
if not df_users_lic.empty and not df_skus.empty:
    df_resumo_licencas = (
        df_users_lic.groupby(["skuPartNumber", "skuId"], as_index=False)
        .agg(qtd_usuarios=("userId", "nunique"))
        .merge(
            df_skus[["skuId", "capacidade", "consumido", "sobra"]],
            on="skuId",
            how="left",
        )
    )
else:
    df_resumo_licencas = pd.DataFrame(
        columns=["skuPartNumber", "skuId", "qtd_usuarios", "capacidade", "consumido", "sobra"]
    )

inventario = build_user_inventory(period="D180", inactive_days=30)

# KPIs para os cards
total_capacidade = int(df_skus["capacidade"].sum()) if not df_skus.empty else 0
total_consumido = int(df_skus["consumido"].sum()) if not df_skus.empty else 0
total_sobra = int(df_skus["sobra"].sum()) if not df_skus.empty else 0

# ====================================================
# DASH APP
# ====================================================

external_stylesheets = [dbc.themes.CYBORG]  # tema escuro
app = Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

import dash_auth

VALID_USERNAME_PASSWORD_PAIRS = {
    os.getenv("DASH_USER", "admin"): os.getenv("DASH_PASS", "C0mp@admin")
}

auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)

# Estilos para DataTable em tema escuro
datatable_dark_style = dict(
    style_header={
        "backgroundColor": "#111111",
        "color": "white",
        "fontWeight": "bold",
    },
    style_cell={
        "backgroundColor": "#222222",
        "color": "white",
        "border": "1px solid #444",
        "fontFamily": "Segoe UI, sans-serif",
        "fontSize": 12,
    },
)

app.layout = dbc.Container(
    [
        html.H2(
            "Governança M365 / SharePoint",
            className="mt-4 mb-4",
            style={"textAlign": "center", "color": "#ffffff"},
        ),
        dbc.Tabs(
            [
                # ----------------------------- TAB 1: Licenças -----------------------------
                dbc.Tab(
                    label="Licenças M365",
                    children=[
                        html.Br(),
                        # Cards de indicadores
                        dbc.Row(
                            [
                                dbc.Col(
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.H6(
                                                    "Capacidade contratada",
                                                    className="card-title",
                                                    style={"color": "#BBBBBB"},
                                                ),
                                                html.H3(
                                                    f"{total_capacidade}",
                                                    style={
                                                        "color": "#00E5FF",
                                                        "fontWeight": "bold",
                                                    },
                                                ),
                                            ]
                                        ),
                                        className="mb-3",
                                        style={
                                            "backgroundColor": "#1E1E1E",
                                            "border": "1px solid #333",
                                        },
                                    ),
                                    md=4,
                                ),
                                dbc.Col(
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.H6(
                                                    "Licenças em uso",
                                                    className="card-title",
                                                    style={"color": "#BBBBBB"},
                                                ),
                                                html.H3(
                                                    f"{total_consumido}",
                                                    style={
                                                        "color": "#FFEA00",
                                                        "fontWeight": "bold",
                                                    },
                                                ),
                                            ]
                                        ),
                                        className="mb-3",
                                        style={
                                            "backgroundColor": "#1E1E1E",
                                            "border": "1px solid #333",
                                        },
                                    ),
                                    md=4,
                                ),
                                dbc.Col(
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.H6(
                                                    "Licenças sobrando",
                                                    className="card-title",
                                                    style={"color": "#BBBBBB"},
                                                ),
                                                html.H3(
                                                    f"{total_sobra}",
                                                    style={
                                                        "color": "#69F0AE",
                                                        "fontWeight": "bold",
                                                    },
                                                ),
                                            ]
                                        ),
                                        className="mb-3",
                                        style={
                                            "backgroundColor": "#1E1E1E",
                                            "border": "1px solid #333",
                                        },
                                    ),
                                    md=4,
                                ),
                            ]
                        ),
                        html.Br(),
                        html.H4("Resumo por SKU", style={"color": "#ffffff"}),
                        dash_table.DataTable(
                            id="tbl-resumo-licencas",
                            columns=[
                                {"name": "SKU", "id": "skuPartNumber"},
                                {"name": "Usuários", "id": "qtd_usuarios"},
                                {"name": "Capacidade", "id": "capacidade"},
                                {"name": "Consumido", "id": "consumido"},
                                {"name": "Sobra", "id": "sobra"},
                            ],
                            data=df_resumo_licencas.to_dict("records"),
                            page_size=10,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            **datatable_dark_style,
                        ),
                        html.Br(),
                        html.H4("Detalhe por SKU", style={"color": "#ffffff"}),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dcc.Dropdown(
                                            id="sku-dropdown",
                                            options=[
                                                {"label": s, "value": s}
                                                for s in sorted(
                                                    df_resumo_licencas["skuPartNumber"]
                                                    .dropna()
                                                    .unique()
                                                )
                                            ]
                                            if not df_resumo_licencas.empty
                                            else [],
                                            value=(
                                                df_resumo_licencas["skuPartNumber"]
                                                .dropna()
                                                .iloc[0]
                                                if not df_resumo_licencas.empty
                                                else None
                                            ),
                                            placeholder="Selecione um SKU",
                                            style={"color": "#000000"},
                                        )
                                    ],
                                    width=4,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [dcc.Graph(id="graf-licenca-sku")],
                                    width=6,
                                ),
                                dbc.Col(
                                    [
                                        dash_table.DataTable(
                                            id="tbl-usuarios-sku",
                                            columns=[
                                                {
                                                    "name": "Usuário",
                                                    "id": "userPrincipalName",
                                                },
                                                {
                                                    "name": "Nome",
                                                    "id": "displayName",
                                                },
                                            ],
                                            page_size=10,
                                            sort_action="native",
                                            filter_action="native",
                                            style_table={"overflowX": "auto"},
                                            **datatable_dark_style,
                                        )
                                    ],
                                    width=6,
                                ),
                            ]
                        ),
                        html.Br(),
                        html.H4(
                            "Inventário de usuários x licenças",
                            style={"color": "#ffffff"},
                        ),
                        dash_table.DataTable(
                            id="tbl-inventario-licencas",
                            columns=[
                                {"name": c, "id": c} for c in inventario.columns
                            ],
                            data=inventario.to_dict("records"),
                            page_size=15,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            **datatable_dark_style,
                        ),
                    ],
                ),
                # ----------------------------- TAB 2: Storage SharePoint -----------------------------
                dbc.Tab(
                    label="Uso de Storage SharePoint",
                    children=[
                        html.Br(),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label(
                                            "Período do relatório (Graph Reports)",
                                            style={"color": "#ffffff"},
                                        ),
                                        dcc.Dropdown(
                                            id="periodo-storage",
                                            options=[
                                                {"label": "Últimos 7 dias", "value": "D7"},
                                                {"label": "Últimos 30 dias", "value": "D30"},
                                                {"label": "Últimos 90 dias", "value": "D90"},
                                                {"label": "Últimos 180 dias", "value": "D180"},
                                            ],
                                            value="D30",
                                            clearable=False,
                                            style={"color": "#000000"},
                                        ),
                                    ],
                                    width=4,
                                ),
                            ],
                            className="mb-3",
                        ),
                        # ======= ROW COM 2 CARDS (TREND + RESUMO) =======
                        dbc.Row(
                            [
                                dbc.Col(
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.H5("Tendência de uso", style={"color": "#ffffff"}),
                                                dcc.Graph(id="graf-storage-trend", config={"displayModeBar": False}),
                                            ]
                                        ),
                                        style={"backgroundColor": "#1E1E1E", "border": "1px solid #333"},
                                        className="mb-3",
                                    ),
                                    md=8,
                                ),
                                dbc.Col(
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.H5("Uso total", style={"color": "#ffffff"}),
                                                html.Div(
                                                    [
                                                        html.Div(id="txt-storage-pct", style={"fontSize": "28px", "fontWeight": "bold", "color": "white"}),
                                                        html.Div(id="txt-storage-tb", style={"fontSize": "14px", "color": "#CCCCCC", "marginBottom": "12px"}),
                                                        dbc.Progress(
                                                            id="prog-storage",
                                                            value=0,
                                                            max=100,
                                                            striped=False,
                                                            animated=False,
                                                            style={"height": "18px"},
                                                            className="mb-2",
                                                        ),
                                                        html.Div(id="txt-storage-restante", style={"fontSize": "12px", "color": "#AAAAAA"}),
                                                    ]
                                                ),
                                            ]
                                        ),
                                        style={"backgroundColor": "#1E1E1E", "border": "1px solid #333"},
                                        className="mb-3",
                                    ),
                                    md=4,
                                ),
                            ]
                        ),
                    ],
                ),
                # ----------------------------- TAB 3: Inventário Usuários -----------------------------
                dbc.Tab(
                    label="Inventário de Usuários",
                    children=[
                        html.Br(),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Visão", style={"color": "#ffffff"}),
                                        dcc.Dropdown(
                                            id="visao-usuarios",
                                            options=[
                                                {
                                                    "label": "Ativos no SharePoint",
                                                    "value": "ativos",
                                                },
                                                {
                                                    "label": "Inativos no SharePoint",
                                                    "value": "inativos",
                                                },
                                                {
                                                    "label": "Bloqueados",
                                                    "value": "bloqueados",
                                                },
                                                {
                                                    "label": "Consomem licença",
                                                    "value": "consomem",
                                                },
                                                {
                                                    "label": "Sem licença",
                                                    "value": "sem_licenca",
                                                },
                                                {
                                                    "label": "Nunca acessaram SP (licenciados)",
                                                    "value": "nunca_acessou",
                                                },
                                                {
                                                    "label": "Candidatos a perder licença",
                                                    "value": "perder_licenca",
                                                },
                                                {
                                                    "label": "Bloqueados consumindo licença",
                                                    "value": "bloq_consumindo",
                                                },
                                            ],
                                            value="perder_licenca",
                                            clearable=False,
                                            style={"color": "#000000"},
                                        ),
                                    ],
                                    width=4,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dash_table.DataTable(
                            id="tbl-usuarios-inventario",
                            page_size=15,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            **datatable_dark_style,
                        ),
                    ],
                ),
                # ----------------------------- TAB 4: Sites SharePoint -----------------------------
                dbc.Tab(
                    label="Sites SharePoint",
                    children=[
                        html.Br(),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Período", style={"color": "#ffffff"}),
                                        dcc.Dropdown(
                                            id="periodo-sites",
                                            options=[
                                                {
                                                    "label": "Últimos 30 dias",
                                                    "value": "D30",
                                                },
                                                {
                                                    "label": "Últimos 90 dias",
                                                    "value": "D90",
                                                },
                                                {
                                                    "label": "Últimos 180 dias",
                                                    "value": "D180",
                                                },
                                            ],
                                            value="D30",
                                            clearable=False,
                                            style={"color": "#000000"},
                                        ),
                                    ],
                                    width=3,
                                ),
                                dbc.Col(
                                    [
                                        html.Label(
                                            "Limiar quota (%)",
                                            style={"color": "#ffffff"},
                                        ),
                                        dcc.Slider(
                                            id="limiar-quota",
                                            min=50,
                                            max=100,
                                            step=5,
                                            value=80,
                                            marks={
                                                50: "50%",
                                                60: "60%",
                                                70: "70%",
                                                80: "80%",
                                                90: "90%",
                                                100: "100%",
                                            },
                                        ),
                                    ],
                                    width=5,
                                ),
                                dbc.Col(
                                    [
                                        html.Label(
                                            "Mínimo compartilhamentos externos",
                                            style={"color": "#ffffff"},
                                        ),
                                        dcc.Input(
                                            id="min-external-shares",
                                            type="number",
                                            value=1,
                                            min=1,
                                            step=1,
                                            style={"width": "100%"},
                                        ),
                                    ],
                                    width=4,
                                ),
                            ],
                            className="mb-4",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.H5(
                                            "Sites próximos/estourando quota",
                                            style={"color": "#ffffff"},
                                        ),
                                        dcc.Graph(id="graf-sites-quota"),
                                        dash_table.DataTable(
                                            id="tbl-sites-quota",
                                            page_size=10,
                                            sort_action="native",
                                            filter_action="native",
                                            style_table={"overflowX": "auto"},
                                            **datatable_dark_style,
                                        ),
                                    ],
                                    width=6,
                                ),
                                dbc.Col(
                                    [
                                        html.H5(
                                            "Sites com compartilhamento externo",
                                            style={"color": "#ffffff"},
                                        ),
                                        dcc.Graph(id="graf-sites-external"),
                                        dash_table.DataTable(
                                            id="tbl-sites-external",
                                            page_size=10,
                                            sort_action="native",
                                            filter_action="native",
                                            style_table={"overflowX": "auto"},
                                            **datatable_dark_style,
                                        ),
                                    ],
                                    width=6,
                                ),
                            ]
                        ),
                        html.Br(),
                        html.H5(
                            "Sites órfãos (sem owner e inativos)",
                            style={"color": "#ffffff"},
                        ),
                        dash_table.DataTable(
                            id="tbl-sites-orfaos",
                            page_size=10,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            **datatable_dark_style,
                        ),
                    ],
                ),
            ]
        ),
    ],
    fluid=True,
    style={
        "backgroundColor": "#121212",
        "color": "#ffffff",
        "minHeight": "100vh",
        "paddingBottom": "40px",
    },
)


# ===================== CALLBACKS =====================

@app.callback(
    Output("graf-storage-trend", "figure"),
    Output("prog-storage", "value"),
    Output("txt-storage-pct", "children"),
    Output("txt-storage-tb", "children"),
    Output("txt-storage-restante", "children"),
    Input("periodo-storage", "value"),
)
def atualizar_grafico_storage(periodo):
    # --------- Trend ----------
    df = metric_storage_trend(periodo)
    if df.empty:
        fig_trend = px.line(title="Nenhum dado retornado")
    else:
        fig_trend = px.line(
            df,
            x="Report Date",
            y="Storage Used (GB)",
            title=f"Uso de Storage do SharePoint ({periodo})",
        )
        fig_trend.update_layout(xaxis_title="Data", yaxis_title="Storage (GB)")

    # --------- Uso total ----------
    resumo = metric_storage_total(periodo)

    # Se faltou SPO_TOTAL_TB no .env ou não veio dado do report
    if resumo["percent_used"] is None:
        return fig_trend, 0, "Sem dados", "Defina SPO_TOTAL_TB no .env (ex: 4.42)", ""

    pct = round(resumo["percent_used"], 2)
    used_tb = resumo["used_tb"]
    alloc_tb = resumo["allocated_tb"]
    free_tb = max(alloc_tb - used_tb, 0)
    free_gb = free_tb * 1024

    txt_pct = f"{pct}% do armazenamento total usado"
    txt_tb = f"{used_tb:.2f} TB / {alloc_tb:.2f} TB usados"
    txt_rest = f"Disponível: {free_gb:.2f} GB"

    return fig_trend, pct, txt_pct, txt_tb, txt_rest

@app.callback(
    Output("graf-licenca-sku", "figure"),
    Output("tbl-usuarios-sku", "data"),
    Input("sku-dropdown", "value"),
)
def atualizar_detalhe_sku(sku_partnumber):
    if not sku_partnumber or df_resumo_licencas.empty:
        return px.bar(title="Nenhum SKU selecionado"), []

    linha = df_resumo_licencas[
        df_resumo_licencas["skuPartNumber"] == sku_partnumber
    ]
    if linha.empty:
        return px.bar(title="SKU não encontrado"), []

    linha = linha.iloc[0]

    df_bar = pd.DataFrame(
        {
            "Categoria": ["Capacidade", "Consumido", "Sobra", "Usuários"],
            "Valor": [
                linha["capacidade"],
                linha["consumido"],
                linha["sobra"],
                linha["qtd_usuarios"],
            ],
        }
    )

    fig = px.bar(
        df_bar,
        x="Categoria",
        y="Valor",
        title=f"Resumo da licença: {sku_partnumber}",
        text="Valor",
    )
    fig.update_traces(textposition="outside")

    df_users_sku = df_users_lic[df_users_lic["skuPartNumber"] == sku_partnumber]
    df_users_sku = df_users_sku[
        ["userPrincipalName", "displayName"]
    ].drop_duplicates()

    return fig, df_users_sku.to_dict("records")


@app.callback(
    Output("tbl-usuarios-inventario", "columns"),
    Output("tbl-usuarios-inventario", "data"),
    Input("visao-usuarios", "value"),
)
def atualizar_tabela_inventario(visao):
    if inventario.empty:
        return [], []

    if visao == "ativos":
        df_view = view_usuarios_ativos(inventario)
    elif visao == "inativos":
        df_view = view_usuarios_inativos(inventario)
    elif visao == "bloqueados":
        df_view = view_usuarios_bloqueados(inventario)
    elif visao == "consomem":
        df_view = view_quem_consome_licenca(inventario)
    elif visao == "sem_licenca":
        df_view = view_quem_sem_licenca(inventario)
    elif visao == "nunca_acessou":
        df_view = view_quem_nunca_acessou(inventario)
    elif visao == "perder_licenca":
        df_view = view_quem_deveria_perder_licenca(inventario)
    elif visao == "bloq_consumindo":
        df_view = view_usuarios_bloqueados_consumindo_licenca(inventario)
    else:
        df_view = inventario.copy()

    cols = [{"name": c, "id": c} for c in df_view.columns]
    data = df_view.to_dict("records")
    return cols, data


@app.callback(
    Output("tbl-sites-quota", "columns"),
    Output("tbl-sites-quota", "data"),
    Output("graf-sites-quota", "figure"),
    Output("tbl-sites-external", "columns"),
    Output("tbl-sites-external", "data"),
    Output("graf-sites-external", "figure"),
    Output("tbl-sites-orfaos", "columns"),
    Output("tbl-sites-orfaos", "data"),
    Input("periodo-sites", "value"),
    Input("limiar-quota", "value"),
    Input("min-external-shares", "value"),
)
def atualizar_sites(periodo, limiar_quota, min_external_shares):
    limiar_quota = limiar_quota or 80.0
    min_external_shares = int(min_external_shares or 1)

    # Quota
    df_quota = metric_sites_near_quota(periodo, limiar_percentual=limiar_quota)
    if df_quota.empty:
        cols_quota = []
        data_quota = []
        fig_quota = px.bar(title="Nenhum site acima do limiar de quota")
    else:
        cols_quota = [{"name": c, "id": c} for c in df_quota.columns]
        data_quota = df_quota.to_dict("records")
        top_q = df_quota.head(20)
        fig_quota = px.bar(
            top_q.sort_values("Uso_%", ascending=False),
            x="Site URL",
            y="Uso_%",
            title=f"Sites por uso de quota (>{limiar_quota:.0f}%)",
        )
        fig_quota.update_layout(xaxis_tickangle=-45)

    # Compartilhamento externo
    df_ext = metric_sites_with_external_sharing(periodo, min_external_shares)
    if df_ext.empty:
        cols_ext = []
        data_ext = []
        fig_ext = px.bar(title="Nenhum site com compartilhamento externo no critério")
    else:
        cols_ext = [{"name": c, "id": c} for c in df_ext.columns]
        data_ext = df_ext.to_dict("records")
        top_e = df_ext.head(20)
        fig_ext = px.bar(
            top_e.sort_values("Compart_Externos", ascending=False),
            x="Site URL",
            y="Compart_Externos",
            title=f"Sites por compartilhamentos externos (mín. {min_external_shares})",
        )
        fig_ext.update_layout(xaxis_tickangle=-45)

    # Sites órfãos
    df_orfaos = metric_orphan_sites(period=periodo, inactive_days=180)
    if df_orfaos.empty:
        cols_orf = []
        data_orf = []
    else:
        cols_orf = [{"name": c, "id": c} for c in df_orfaos.columns]
        data_orf = df_orfaos.to_dict("records")

    return (
        cols_quota,
        data_quota,
        fig_quota,
        cols_ext,
        data_ext,
        fig_ext,
        cols_orf,
        data_orf,
    )


if __name__ == "__main__":
    app.run(debug=True, port=8050)
