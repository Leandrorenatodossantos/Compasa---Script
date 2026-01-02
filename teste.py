import dash
from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

# 1. PREPARAÇÃO DOS DADOS (Simulação)
# Criamos uma tabelinha simples para o exemplo funcionar
df = pd.DataFrame({
    "Cidade": ["Curitiba", "Curitiba", "Curitiba", "São Paulo", "São Paulo", "São Paulo"],
    "Data": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-01", "2023-01-02", "2023-01-03"],
    "Temperatura": [20, 22, 18, 28, 29, 27]
})

# 2. INICIALIZAÇÃO DO APP
# Aqui aplicamos o tema Bootstrap (CSS)
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.PULSE])

# 3. O LAYOUT (A "Fachada" Visual)
app.layout = dbc.Container(children=[
    
    # Título do App
    dbc.Row([
        dbc.Col(
            html.H1("Meu Painel do Clima", style={"textAlign": "center"}),
            width=12
        )
    ], justify="center"),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Selecione a Cidade", className="card-title", style={"textAlign": "center"}),
                    dcc.Dropdown(
                        id='seletor-cidade',  
                        options=[
                            {'label': 'Curitiba', 'value': 'Curitiba'},
                            {'label': 'São Paulo', 'value': 'São Paulo'}
                        ],
                        value='Curitiba',    # Valor inicial
                        clearable=False
                    )
                ])
            ], color="light", style={
                "borderRadius": "8px",
                "border": "0.5px solid #007bff"
            }) # Cor de fundo do card, cantos arredondados e linha azul
        ], width=2),

        # Coluna Direita: O Gráfico
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    id="titulo-grafico-temperatura",
                    children="Temperaturas", 
                    style={
                        "backgroundColor": "#ff0000c1",
                        "color": "white",
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "borderTopLeftRadius": "8px",
                        "borderTopRightRadius": "8px"
                    }
                ),
                dbc.CardBody([
                    dcc.Graph(id='grafico-temperatura', figure={})
                ], style={"height": "30%", "width": "100%"})
            ], style={
                "borderRadius": "8px",
                "border": "1px solid #007bff"
            })
        ], width=5),
        dbc.Col([
            dbc.Card([
                    dbc.CardHeader(
                    id="Grafico 2",
                    children="Grafico 2", 
                    style={
                        "backgroundColor": "#ff0000c1",
                        "color": "white",
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "borderTopLeftRadius": "8px",
                        "borderTopRightRadius": "8px"
                    }
                ),
                dbc.CardBody([
                    dcc.Graph(id='grafico-temperatura2', figure={})
                ], style={"height": "20%", "width": "100%"})
            ], style={
                "borderRadius": "8px",
                "border": "1px solid #007bff"
            })
        ], width=5)
    ])
], 
    fluid=True)

# 4. O CALLBACK (A "Casa de Máquinas")
@app.callback(
    Output('grafico-temperatura', 'figure'), # Destino: O Gráfico
    Input('seletor-cidade', 'value')          # Gatilho: O Dropdown
)
def atualizar_grafico(cidade_escolhida):
    # Filtragem
    df_filtrado = df[df['Cidade'] == cidade_escolhida]
    
    # Criação do Gráfico
    fig = px.bar(
        df_filtrado,
        x='Temperatura',
        y='Data',
        orientation='h',
        ##title=f'Temperaturas em {cidade_escolhida}',
        # text='Temperatura',
        height=300 # Altura reduzida para metade do card
    )
    fig.update_traces(textposition='outside')  # Posição do texto fora da barra
    return fig

# 5. RODAR O SERVIDOR
if __name__ == '__main__':
    app.run(debug=True)
    
    
    
    
