import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State
import plotly.io as pio
from dotenv import load_dotenv
import requests
from concurrent.futures import ThreadPoolExecutor  

pio.templates.default = "plotly_dark"

app = Dash(__name__)
server = app.server
load_dotenv()
api_url = os.getenv('API_URL')
authorization_token = os.getenv('AUTHORIZATION_TOKEN')
session = requests.Session()

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f'token {authorization_token}'
}

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


def fetch_page(page, start_date, end_date, page_size):
   
    try:
        filter_query = (f'?fields=["timestamp","extrusion_time","downtime_reasons"]'
                        f'&filters=[["timestamp",">=","{start_date}"],'
                        f'["timestamp","<=","{end_date}"]]'
                        f'&limit={page_size}&offset={page * page_size}')
        
        response = requests.get(f'{api_url}{filter_query}', headers=headers)
        response.raise_for_status()
        
        return response.json().get('data', [])
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page}: {str(e)}")
        return []


def parse_frappe_api(selected_date):
    start_date = f"{selected_date} 04:00:00"
    end_date = f"{selected_date} 17:00:00"
    
    data = []  
    page = 0  
    page_size = 200000

    initial_data = fetch_page(page, start_date, end_date, page_size)
    data.extend(initial_data)
    
      
   
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda p: fetch_page(p, start_date, end_date, page_size), range(page)))

    for page_data in results:
        data.extend(page_data)
        


    df = pd.DataFrame(data)
    print(f'Records for selected date {df.shape}')
    if df.empty:
        return "No data found for the selected date range."
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = pd.to_datetime(df['timestamp'] + timedelta(hours=2))
    df['extrusion_time'] = df['extrusion_time'].apply(cycle_times)

   
    df = df.sort_values(by='timestamp')

    return df

def convert_to_extrusion_time(value):
    return float(value) if value else None

def format_time(hours):
  
    total_minutes = int(hours * 60)
    formatted_hours = total_minutes // 60
    formatted_minutes = total_minutes % 60
    return f"{formatted_hours}:{formatted_minutes:02d}"  

def cycle_times(raw_value):
    seconds_scaling_factor = 1e9
    if pd.notnull(raw_value):
        return (raw_value / seconds_scaling_factor)
    return 0

def format_time(hours):
    """Format hours into a more readable string."""
    total_seconds = int(hours * 3600)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"

def process_and_plot_data(df_cycle):
    df_cycle['Timestamp'] = pd.to_datetime(df_cycle['timestamp'])
    df_cycle['downtime_reasons'] = df_cycle['downtime_reasons']   
    total_hours = 10

    operational_time = ((df_cycle['extrusion_time'] >= 1).sum() * (1 / 60) ) / 60  
    downtime = total_hours - operational_time

    avg_cycle_time_minutes = 3  
    avg_cycle_time_seconds = avg_cycle_time_minutes * 60  
    number_of_cycles = (operational_time * 3600) / avg_cycle_time_seconds
   
   
    print(f'Average Cycle Time: {avg_cycle_time_minutes}')
    print(f'Number of cycles: {avg_cycle_time_seconds}')
    print(f'Operational time: {format_time(operational_time)}')
    print(f'Downtime: {format_time(downtime)}')


    line_fig = go.Figure()
    line_fig.add_trace(go.Scatter(
        x=df_cycle['Timestamp'], 
        y=df_cycle['extrusion_time'], 
        mode='lines', 
        name='Extrusion Time - Operational Time',
        line=dict(shape='linear'),
        hovertext=df_cycle['downtime_reasons'],
        hoverinfo='text+x+y'
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
        df_cycle = parse_frappe_api(selected_date)
        df_cycle = df_cycle.drop_duplicates()
        print(df_cycle.head())  
        print(f'Unique Reseasons:{df_cycle[df_cycle["downtime_reasons"].notna()]}')
        if isinstance(df_cycle, str): 
            return html.Div([html.P(df_cycle)])

        line_fig, bar_fig = process_and_plot_data(df_cycle)

        return [
            dcc.Graph(figure=line_fig),
            dcc.Graph(figure=bar_fig)  
        ]

    return html.Div([html.P("Select a date and press 'Generate Figure'.")])

if __name__ == '__main__':
    app.run_server(debug=True)
