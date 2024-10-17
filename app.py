import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State
import plotly.io as pio

pio.templates.default = "plotly_dark"

app = Dash(__name__)
server = app.server

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
                html.Div(
                    id='output-graph',
                    style={'display': 'flex', 'flex-wrap': 'wrap', 'justify-content': 'space-around'}
                ),
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
    start_date = f"{selected_date} 07:00:00"
    end_date = f"{selected_date} 17:00:00"
    
    try:
        conn = sqlite3.connect(db_path)
        query = f"""
        SELECT 
            strftime('%Y-%m-%d %H:%M', datetime(TS / 1000000, 'unixepoch', 'UTC', '+2 hours')) AS TS,
            Val1 AS Val1,
            Val2 AS Val2,
            Val3 AS Val3
        FROM 
            TblTrendData
        WHERE 
            datetime(TS / 1000000, 'unixepoch', 'UTC', '+2 hours') BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 
            CAST(strftime('%s', datetime(TS / 1000000, 'unixepoch', 'UTC', '+2 hours')) / (1 * 60) AS INTEGER)
        ORDER BY 
            TS;"""

        df = pd.read_sql_query(query, conn)
        conn.close()
        df['Val1'] = df['Val1'].apply(convert_to_pressure)
        df['Val2'] = df['Val2'].apply(convert_to_pressure)
        df['Val3'] = df['Val3'].apply(convert_to_pressure)

        return df
    except sqlite3.DatabaseError as e:
        return f"Database error occurred: {str(e)}"

def format_time(total_hours):
    """Format total hours into H:H (hours:minutes) format."""
    hours = int(total_hours)
    minutes = int((total_hours - hours) * 60)
    return f"{hours}:{minutes:02d}"

def format_time(hours):
    """Format hours in H:MM format."""
    total_minutes = int(hours * 60)
    formatted_hours = total_minutes // 60
    formatted_minutes = total_minutes % 60
    return f"{formatted_hours}:{formatted_minutes:02d}"  

def format_time(hours):
    """Format hours in H:MM format."""
    total_minutes = int(hours * 60)
    formatted_hours = total_minutes // 60
    formatted_minutes = total_minutes % 60
    return f"{formatted_hours}:{formatted_minutes:02d}"

def format_time(hours):
    """Format hours in H:MM format."""
    total_minutes = int(hours * 60)
    formatted_hours = total_minutes // 60
    formatted_minutes = total_minutes % 60
    return f"{formatted_hours}:{formatted_minutes:02d}"

def process_and_plot_data(df_cycle):
    """Process the cycle data and return a Plotly figure for Val1 (line graph) and a bar chart for operational and downtime."""
    df_cycle['Timestamp'] = pd.to_datetime(df_cycle['TS'])

    total_hours = 10 

    operational_time = ((df_cycle['Val1'] > 0.50).sum() / 60)  
    downtime = total_hours - operational_time

    operational_time = min(operational_time, total_hours)
    downtime = max(downtime, 0)

    formatted_operational_time = format_time(operational_time)
    formatted_downtime = format_time(downtime)

    line_fig = go.Figure()
    line_fig.add_trace(go.Scatter(
        x=df_cycle['Timestamp'], 
        y=df_cycle['Val1'], 
        mode='lines', 
        name='Val1 - Operational Time',
        line=dict(shape='linear')
    ))

    date_min = df_cycle['Timestamp'].min().replace(hour=7, minute=0, second=0)
    date_max = df_cycle['Timestamp'].max().replace(hour=17, minute=0, second=0)

    line_fig.update_layout(
        title='Extrusion Time',
        xaxis_title='Timestamp',
        yaxis_title='Cycle Reading',
        legend_title='Cycle Data',
        xaxis_tickformat='%Y-%m-%d %H:%M',
        xaxis=dict(
            range=[date_min, date_max],  
            tickmode='linear',  
            dtick=3600000 * 0.25, 
            tickangle=90 
        ),
        height=450,
        width=850
    )

    bar_fig = go.Figure()
    bar_fig.add_trace(go.Bar(
        x=['Operational Time'],
        y=[operational_time],
        name='Operational Time',
        marker_color='green'
    ))

    bar_fig.add_trace(go.Bar(
        x=['Downtime'],
        y=[downtime],
        name='Downtime',
        marker_color='red'
    ))

    bar_fig.update_layout(
        title='Operational and Downtime Overview',
        xaxis_title='Status',
        yaxis_title='Hours',
        height=450,
        width=450,
        showlegend=True,
        legend=dict(title='Summary of Hours', itemsizing='constant')
    )
    bar_fig.add_annotation(
    text="Total timeframe: 07:00 to 17:00",
    xref="paper", yref="paper",
    x=0.5, y=1.17,
    showarrow=False,
    font=dict(size=12),
    bordercolor='black',
    borderwidth=1,
    borderpad=4,
    )



    bar_fig.for_each_trace(lambda t: t.update(name=f"{t.name}: {format_time(t.y[0])}"))

    return line_fig, bar_fig





@app.callback(
    Output('output-graph', 'children'),
    Input('generate-figure-btn', 'n_clicks'),
    State('date-picker', 'date')
)
def update_output(n_clicks, selected_date):
    if n_clicks > 0:
        cycle_db_path = 'data/Application.E19_Page_1_Trend2.1.sqlite'

        df_cycle = parse_sqlite(cycle_db_path, selected_date)
        #csv_file_path = f'csv_data/cycle-data-created-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
        #df_cycle.to_csv(csv_file_path, index=False)
        
        if isinstance(df_cycle, str): 
            return html.Div([html.P(df_cycle)])

        line_fig, pie_fig = process_and_plot_data(df_cycle)

        return [
            dcc.Graph(figure=line_fig),
            dcc.Graph(figure=pie_fig)
        ]

    return html.Div([html.P("Select a date and press 'Generate Figure'.")])

if __name__ == '__main__':
    app.run_server(debug=True)
