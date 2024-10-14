import sqlite3
import pandas as pd
from datetime import datetime, time
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, callback, State
import base64
import io
import plotly.io as pio

pio.templates.default = "plotly_dark"

# Initialize Dash App
app = Dash(__name__)
server = app.server

# Reusable upload styles
upload_style = {
    'width': '100%', 'height': '60px', 'lineHeight': '60px',
    'borderWidth': '1px', 'borderStyle': 'dashed',
    'borderRadius': '5px', 'textAlign': 'center',
    'margin': '10px', 'display': 'flex',
    'justifyContent': 'center', 'alignItems': 'center',
}

app.layout = html.Div(
    className='app-container',
    children=[
        html.H1("Aluecor Press & Aging Oven Dashboard", className='app-heading'),

        # File Uploads
        dcc.Upload(id='upload-data-cycle', children=html.Div(['Drag and Drop or ', html.A('Select Cycle Data SQLite File')]), style=upload_style),
        html.Div(id='cycle-upload-status'),

        dcc.Upload(id='upload-data-thermocouple', children=html.Div(['Drag and Drop or ', html.A('Select Thermocouple Data SQLite File')]), style=upload_style),
        html.Div(id='thermocouple-upload-status'),

        # Date Picker Range
        dcc.DatePickerRange(id='date-picker-range', start_date=datetime(2024, 10, 7), end_date=datetime(2024, 10, 8), display_format='YYYY-MM-DD', style={'margin': '20px'}),

        # Generate Button and Output Graphs
        html.Button('Generate Figure', id='generate-figure-btn', n_clicks=0),
        html.Div(id='loading-status'),  
        dcc.Loading(id='loading-graphs', type='circle', children=[html.Div(id='output-graph')]),
    ]
)

def convert_to_pressure(raw_value):
    """Convert raw pressure values to human-readable format."""
    scaling_factor = 1e6
    return raw_value / scaling_factor if pd.notnull(raw_value) else 0.0

def parse_sqlite(contents, start_date, end_date):
    """Parse SQLite file and return a DataFrame."""
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)

    try:
        with sqlite3.connect(':memory:') as conn:
            conn.executescript(decoded.decode('utf-8'))

            query = """
                SELECT 
                    strftime('%Y-%m-%d %H:%M', datetime(TS / 1000000, 'unixepoch', 'UTC')) AS TS,
                    AVG(Val1) AS Val1, AVG(Val2) AS Val2, AVG(Val3) AS Val3
                FROM TblTrendData
                WHERE datetime(TS / 1000000, 'unixepoch', 'UTC') 
                      BETWEEN ? AND ?
                GROUP BY CAST(strftime('%s', datetime(TS / 1000000, 'unixepoch', 'UTC')) / (15 * 60) AS INTEGER)
                ORDER BY TS;
            """
            params = (f"{start_date} 06:00:00", f"{end_date} 18:00:00")
            df = pd.read_sql_query(query, conn, params=params)
            df[['Val1', 'Val2', 'Val3']] = df[['Val1', 'Val2', 'Val3']].applymap(convert_to_pressure)

            return df
    except Exception as e:
        return f"Error parsing cycle data: {str(e)}"

def parse_thermocouple(contents, start_date, end_date):
    """Parse thermocouple SQLite file and return a DataFrame."""
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)

    try:
        with sqlite3.connect(':memory:') as conn:
            conn.executescript(decoded.decode('utf-8'))

            query = """
                SELECT 
                    strftime('%Y-%m-%d %H:%M', datetime(TS / 1000000, 'unixepoch', 'UTC')) AS TS,
                    AVG(Val1) AS Val1, AVG(Val2) AS Val2, AVG(Val3) AS Val3,
                    AVG(Val4) AS Val4, AVG(Val5) AS Val5, AVG(Val6) AS Val6
                FROM TblTrendData
                WHERE datetime(TS / 1000000, 'unixepoch', 'UTC') 
                      BETWEEN ? AND ?
                GROUP BY CAST(strftime('%s', datetime(TS / 1000000, 'unixepoch', 'UTC')) / (15 * 60) AS INTEGER)
                ORDER BY TS;
            """
            params = (f"{start_date} 06:00:00", f"{end_date} 18:00:00")
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        return f"Error parsing thermocouple data: {str(e)}"

def process_and_plot_data(df_cycle, df_thermocouple):
    """Generate Plotly figures from the parsed data."""
    figs = []
    for i, col in enumerate(['Val1', 'Val2', 'Val3'], 1):
        fig = go.Figure(go.Scatter(x=df_cycle['TS'], y=df_cycle[col], mode='lines', name=f'Cycle {col}'))
        fig.update_layout(title=f'Cycle {col}', xaxis_title='Timestamp', yaxis_title='Time (s)')
        figs.append(fig)

    fig4 = go.Figure()
    for col in ['Val1', 'Val2', 'Val3', 'Val4', 'Val5', 'Val6']:

    fig4.add_trace(go.Scatter(x=df_thermocouple['TS'], y=df_thermocouple[col], mode='lines', name=f'Thermocouple {col[-1]}'))
    fig4.update_layout(title='Thermocouple Temperatures', xaxis_title='Timestamp', yaxis_title='Temperature (°C)')
    figs.append(fig4)

    return [dcc.Graph(figure=fig) for fig in figs]

@app.callback(
    Output('output-graph', 'children'),
    Input('generate-figure-btn', 'n_clicks'),
    State('upload-data-cycle', 'contents'),
    State('upload-data-thermocouple', 'contents'),
    State('date-picker-range', 'start_date'),
    State('date-picker-range', 'end_date')
)
def update_output(n_clicks, contents_cycle, contents_thermocouple, start_date, end_date):
    if n_clicks > 0 and contents_cycle and contents_thermocouple:
        df_cycle = parse_sqlite(contents_cycle, start_date, end_date)
        df_thermocouple = parse_thermocouple(contents_thermocouple, start_date, end_date)

        if isinstance(df_cycle, str) or isinstance(df_thermocouple, str):
            return html.Div([html.P("Error in one of the uploaded files.")])

        return process_and_plot_data(df_cycle, df_thermocouple)

    return html.Div([html.P("Upload both files and click 'Generate Figure'.")])

if __name__ == '__main__':
    app.run_server(debug=False)
