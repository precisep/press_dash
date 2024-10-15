import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State
import plotly.io as pio

pio.templates.default = "plotly_dark"

app = Dash(__name__)

app.layout = html.Div(
    className='app-container',
    children=[
        html.H1("Aluecor Press Dashboard", className='app-heading'),

       
        dcc.DatePickerSingle(
            id='date-picker',
            date=(datetime.now() - timedelta(1)).date(),  
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

def parse_sqlite(db_path, selected_date):
    """Parse the SQLite file and return a DataFrame."""
    start_date = f"{selected_date} 06:00:00"
    end_date = f"{selected_date} 19:00:00"
    
    try:
        conn = sqlite3.connect(db_path)
        query = f"""
            SELECT 
                strftime('%Y-%m-%d %H:%M', datetime(TS / 1000000, 'unixepoch', 'UTC')) AS TS,
                AVG(Val1) AS Val1,
                AVG(Val2) AS Val2,
                AVG(Val3) AS Val3
            FROM 
                TblTrendData
            WHERE 
                datetime(TS / 1000000, 'unixepoch', 'UTC') BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 
                CAST(strftime('%s', datetime(TS / 1000000, 'unixepoch', 'UTC')) / (1 * 60) AS INTEGER)
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
        return f"Database error occurred: {str(e)}"

def process_and_plot_data(df_cycle):
    """Process the cycle data and return separate Plotly figures."""
    df_cycle['Timestamp'] = pd.to_datetime(df_cycle['TS'])

    margin = pd.Timedelta(minutes=1) 

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=df_cycle['Timestamp'], y=df_cycle['Val1'], mode='lines', name='Extrusion Time'))
    fig1.update_layout(title='Extrusion Time', xaxis_title='Timestamp', yaxis_title='Time (s)', legend_title='Cycle Data')
    fig1.update_xaxes(range=[df_cycle['Timestamp'].min(), df_cycle['Timestamp'].max() + margin]) 

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df_cycle['Timestamp'], y=df_cycle['Val2'], mode='lines', name='Dead Cycle Time'))
    fig2.update_layout(title='Dead Cycle Time', xaxis_title='Timestamp', yaxis_title='Time (s)', legend_title='Cycle Data')
    fig2.update_xaxes(range=[df_cycle['Timestamp'].min(), df_cycle['Timestamp'].max() + margin])  

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=df_cycle['Timestamp'], y=df_cycle['Val3'], mode='lines', name='Full Cycle Time'))
    fig3.update_layout(title='Full Cycle Time', xaxis_title='Timestamp', yaxis_title='Time (s)', legend_title='Cycle Data')
    fig3.update_xaxes(range=[df_cycle['Timestamp'].min(), df_cycle['Timestamp'].max() + margin]) 

    return [fig1, fig2, fig3]

@app.callback(
    Output('output-graph', 'children'),
    Input('generate-figure-btn', 'n_clicks'),
    State('date-picker', 'date')
)
def update_output(n_clicks, selected_date):
    if n_clicks > 0:
        cycle_db_path = 'data/Application.E19_Page_1_Trend2.1.sqlite'

        df_cycle = parse_sqlite(cycle_db_path, selected_date)

        if isinstance(df_cycle, str): 
            return html.Div([html.P(df_cycle)])

        figures = process_and_plot_data(df_cycle)

        graph_components = [dcc.Graph(figure=fig) for fig in figures]
        return graph_components

    return html.Div([html.P("Select a date and press 'Generate Figure'.")])

if __name__ == '__main__':
    app.run_server(debug=True)
