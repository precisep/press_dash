import sqlite3
import pandas as pd
from datetime import datetime, time
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, callback, State
import base64
import io
import plotly.io as pio


pio.templates.default = "plotly_dark"

app = Dash(__name__)

server = app.server
app.layout = html.Div(
    className='app-container',
    children=[
        html.H1("Aluecor Press & Aging Oven Dashboard", className='app-heading'),

        dcc.Loading(
            id="loading-spinner-upload",
            type="circle", 
            children=[
                dcc.Upload(
                    id='upload-data-cycle',
                    children=html.Div(['Drag and Drop or ', html.A('Select Cycle Data SQLite File')]),
                    style={
                        'width': '100%',
                        'height': '60px',
                        'lineHeight': '60px',
                        'borderWidth': '1px',
                        'borderStyle': 'dashed',
                        'borderRadius': '5px',
                        'textAlign': 'center',
                        'margin': '10px',
                        'display': 'flex',
                        'justifyContent': 'center',
                        'alignItems': 'center',
                    }
                ),
                html.Div(id='cycle-upload-status'),

                dcc.Upload(
                    id='upload-data-thermocouple',
                    children=html.Div(['Drag and Drop or ', html.A('Select Thermocouple Data SQLite File')]),
                    style={
                        'width': '100%',
                        'height': '60px',
                        'lineHeight': '60px',
                        'borderWidth': '1px',
                        'borderStyle': 'dashed',
                        'borderRadius': '5px',
                        'textAlign': 'center',
                        'margin': '10px',
                        'display': 'flex',
                        'justifyContent': 'center',
                        'alignItems': 'center',
                    }
                ),
                html.Div(id='thermocouple-upload-status'),
            ],
            fullscreen=False  
        ),

        dcc.DatePickerRange(
            id='date-picker-range',
            start_date=datetime(2024, 10, 7),
            end_date=datetime(2024, 10, 8),
            display_format='YYYY-MM-DD',
            style={'margin': '20px'}
        ),

        dcc.Loading(
            id="loading-spinner-generate",
            type="circle",  
            children=[
                html.Button('Generate Figure', id='generate-figure-btn', n_clicks=0),
                html.Div(id='loading-status') 
            ],
            fullscreen=False  
        ),

        dcc.Loading(  
            id='loading-graphs',
            type='circle',  
            children=[
                html.Div(id='output-graph'),
            ]
        ),
    ]
)
def convert_to_pressure(raw_value):
    """Convert raw pressure values to human-readable format."""
    scaling_factor = 1e6  
    return raw_value / scaling_factor if pd.notnull(raw_value) else 0.0

def parse_sqlite(contents,start_date,end_date):
    """Parse the SQLite file and return a DataFrame."""
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)

    try:
        with open('uploaded_db_cycle.sqlite', 'wb') as f:
            f.write(decoded)

        conn = sqlite3.connect('uploaded_db_cycle.sqlite')
        query = f"""
            SELECT 
                strftime('%Y-%m-%d %H:%M', datetime(TS / 1000000, 'unixepoch', 'UTC')) AS TS,
                AVG(Val1) AS Val1,
                AVG(Val2) AS Val2,
                AVG(Val3) AS Val3
            FROM 
                TblTrendData
            WHERE 
                datetime(TS / 1000000, 'unixepoch', 'UTC') BETWEEN '{start_date} 06:00:00' AND '{end_date} 18:00:00'
            GROUP BY 
                CAST(strftime('%s', datetime(TS / 1000000, 'unixepoch', 'UTC')) / (15 * 60) AS INTEGER)
            ORDER BY 
                TS;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        df['Val1'] = df['Val1'].apply(convert_to_pressure)
        df['Val2'] = df['Val2'].apply(convert_to_pressure)
        df['Val3'] = df['Val3'].apply(convert_to_pressure)

        return df
    except sqlite3.DatabaseError as e:
        return f"Error: {str(e)}"

def parse_thermocouple(contents,start_date,end_date):
    """Parse the thermocouple SQLite file and return a DataFrame."""
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)

    try:
        with open('uploaded_db_thermocouple.sqlite', 'wb') as temp_db:
            temp_db.write(decoded)

        conn = sqlite3.connect('uploaded_db_thermocouple.sqlite')
        query = f"""
            SELECT 
                strftime('%Y-%m-%d %H:%M', datetime(TS / 1000000, 'unixepoch', 'UTC')) AS TS,
                AVG(Val1) AS Val1,
                AVG(Val2) AS Val2,
                AVG(Val3) AS Val3,
                AVG(Val4) AS Val4,
                AVG(Val5) AS Val5,
                AVG(Val6) AS Val6
            FROM 
                TblTrendData
            WHERE 
                datetime(TS / 1000000, 'unixepoch', 'UTC') BETWEEN '{start_date} 06:00:00' AND '{end_date} 18:00:00'
            GROUP BY 
                CAST(strftime('%s', datetime(TS / 1000000, 'unixepoch', 'UTC')) / (15 * 60) AS INTEGER)
            ORDER BY 
                TS;
        """
        df_thermocouple = pd.read_sql_query(query, conn)
        conn.close()
        return df_thermocouple

    except sqlite3.DatabaseError as e:
        return f"Error: {str(e)}"


def convert_micro_to_datetime(ts):
    """Convert microseconds to a human-readable datetime."""
    return datetime.utcfromtimestamp(ts / 1e6).strftime('%Y-%m-%d %H:%M:%S')

def process_and_plot_data(df_cycle, df_thermocouple, start_date, end_date):
    """Process the cycle data and thermocouple data and return separate Plotly figures."""

    df_cycle['Timestamp'] = df_cycle['TS']
    df_thermocouple['Timestamp'] = df_thermocouple['TS']

 
    df_cycle['Timestamp'] = pd.to_datetime(df_cycle['Timestamp'])
    df_thermocouple['Timestamp'] = pd.to_datetime(df_thermocouple['Timestamp'])


    filtered_cycle = df_cycle[(df_cycle['Timestamp'] >= start_date) & (df_cycle['Timestamp'] <= end_date)]
    filtered_thermocouple = df_thermocouple[(df_thermocouple['Timestamp'] >= start_date) & (df_thermocouple['Timestamp'] <= end_date)]

    
    filtered_cycle = filtered_cycle[filtered_cycle['Timestamp'].dt.time.between(time(6, 0), time(18, 0))]
    filtered_thermocouple = filtered_thermocouple[filtered_thermocouple['Timestamp'].dt.time.between(time(6, 0), time(18, 0))]

  
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=filtered_cycle['Timestamp'], y=filtered_cycle['Val1'], mode='lines', name='Extrusion Time'))
    fig1.update_layout(title='Extrusion Time', xaxis_title='Timestamp', yaxis_title='Time (s)', legend_title='Cycle Data')

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=filtered_cycle['Timestamp'], y=filtered_cycle['Val2'], mode='lines', name='Dead Cycle Time'))
    fig2.update_layout(title='Dead Cycle Time', xaxis_title='Timestamp', yaxis_title='Time (s)', legend_title='Cycle Data')

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=filtered_cycle['Timestamp'], y=filtered_cycle['Val3'], mode='lines', name='Full Cycle Time'))
    fig3.update_layout(title='Full Cycle Time', xaxis_title='Timestamp', yaxis_title='Time (s)', legend_title='Cycle Data')

    fig4 = go.Figure()
    for col in ['Val1', 'Val2', 'Val3', 'Val4', 'Val5', 'Val6']:
        fig4.add_trace(go.Scatter(x=filtered_thermocouple['Timestamp'], y=filtered_thermocouple[col], mode='lines', name=f'Thermocouple {col}'))
    fig4.update_layout(title='Thermocouple Temperatures', xaxis_title='Timestamp', yaxis_title='Temperature (°C)', legend_title='Thermocouples')

    for fig in [fig1, fig2, fig3, fig4]:
        fig.update_xaxes(tickformat="%Y-%m-%d %H:%M")

    return [fig1, fig2, fig3, fig4]

@app.callback(
    [Output('cycle-upload-status', 'children'),
     Output('thermocouple-upload-status', 'children')],
    [Input('upload-data-cycle', 'contents'),
     Input('upload-data-thermocouple', 'contents')]
)
def show_upload_status(contents_cycle, contents_thermocouple):
    cycle_status = "Cycle data uploaded successfully!" if contents_cycle else "No cycle data uploaded yet."
    thermocouple_status = "Thermocouple data uploaded successfully!" if contents_thermocouple else "No thermocouple data uploaded yet."
    return cycle_status, thermocouple_status

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
        df_cycle = parse_sqlite(contents_cycle,start_date,end_date)
        df_thermocouple = parse_thermocouple(contents_thermocouple,start_date,end_date)

        if isinstance(df_cycle, str) or isinstance(df_thermocouple, str):
            return html.Div([html.P("Error in one of the uploaded files.")])

        figures = process_and_plot_data(df_cycle, df_thermocouple, pd.to_datetime(start_date), pd.to_datetime(end_date))
        
        graph_components = [dcc.Graph(figure=fig) for fig in figures]
        return graph_components  

    return html.Div([html.P("Upload both files, select date range, and press 'Generate Figure'.")])

if __name__ == '__main__':
    app.run_server(debug=False)
