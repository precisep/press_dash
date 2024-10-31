import os
import sqlite3
import pandas as pd
import dash
from dash import Dash, dcc, html, Input, Output, State
from concurrent.futures import ThreadPoolExecutor  
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv
import plotly.io as pio
import requests
import time

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
        html.H1("Aluecor Alarms Dashboard", className='app-heading'),

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
        filter_query = (f'?fields=["tslast","tsactive","alarm","time_difference_minutes"]'
                        f'&filters=[["tsactive",">=","{start_date}"],'
                        f'["tsactive","<=","{end_date}"]]'
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
        results = list(executor.map(lambda p: fetch_page(p, start_date, end_date, page_size), range(page + 1)))

    for page_data in results:
        data.extend(page_data)

    df = pd.DataFrame(data)
    df['tsactive'] = pd.to_datetime(df['tsactive'])  # Ensure tsactive is in datetime format
    print(f'Records for selected date {df.shape}')
    if df.empty:
        return "No data found for the selected date range."
    
    return df

def create_figure(selected_date, df):
    start_date = f"{selected_date} 07:00:00"
    end_date = f"{selected_date} 17:00:00"
    
    start_date_filter = pd.to_datetime(start_date)
    end_date_filter = pd.to_datetime(end_date)

    mask = (df['tsactive'] >= start_date_filter) & (df['tsactive'] < end_date_filter)
    filtered_df = df[mask]

    filtered_df['Equipment Group'] = filtered_df['alarm'].apply(lambda alarm: map_to_equipment_group(alarm, equipment_grouping)).dropna()

    alarm_downtime_totals = filtered_df.groupby(['Equipment Group'])['time_difference_minutes'].sum().reset_index()
    alarm_downtime_totals = alarm_downtime_totals.sort_values('time_difference_minutes', ascending=False)
    filtered_df['Equipment Group'] = pd.Categorical(filtered_df['Equipment Group'], categories=alarm_downtime_totals['Equipment Group'], ordered=True)
    filtered_df = filtered_df.sort_values('Equipment Group', ascending=False)
    filtered_df['tslast'] = pd.to_datetime(filtered_df['tslast'])
    time_range = pd.date_range(start=start_date, end=end_date, freq='h')

    alarm_group = filtered_df['Equipment Group'].unique()
    fig = go.Figure()
    bar_width = 0.25

    for equipment_group in alarm_group:
        alarm_data = filtered_df[filtered_df['Equipment Group'] == equipment_group]
        last_time = pd.Timestamp(start_date)

        for hour in time_range:
            active_alarms = alarm_data[(alarm_data['tslast'] >= last_time) & (alarm_data['tslast'] < hour)]
            downtime_total = active_alarms['time_difference_minutes'].sum()
            alarms_list = active_alarms['alarm'].unique()

            if downtime_total > 0:
                if (hour - last_time).total_seconds() / 60 > 0:
                    active_time = (hour - last_time).total_seconds() / 60 - downtime_total
                    start_time = active_alarms['tsactive'].iloc[0] if not active_alarms.empty else last_time
                    
                    start_time_in_minutes = (start_time - pd.Timestamp(start_time.date())).total_seconds() / 60
                    
                    alarms_hover = "<br>".join(alarms_list)

                    fig.add_trace(go.Bar(
                        y=[equipment_group],
                        x=[active_time],
                        width=bar_width,
                        orientation='h',
                        name='Good State',
                        marker_color='lightgreen',
                        base=last_time.hour * 60 + last_time.minute,
                        hovertemplate=f'Start Time: {start_time.strftime("%Y-%m-%d %H:%M")} <br>Equipment: {equipment_group} Type: Good State<br>Duration: {minutes_to_hhmm(round(active_time, 2))}<extra></extra>',
                        showlegend=False
                    ))

                    alarm_start_time_in_minutes = last_time.hour * 60 + last_time.minute + active_time
                    alarm_start_time = last_time + pd.to_timedelta(active_time, unit='m')  
                 
                    fig.add_trace(go.Bar(
                        y=[equipment_group],
                        x=[downtime_total],
                        width=bar_width,
                        orientation='h',
                        name='Active Alarm',
                        marker_color='red',
                        base=last_time.hour * 60 + last_time.minute + active_time,
                        hovertemplate=f'Start Time: {alarm_start_time.strftime("%Y-%m-%d %H:%M")} <br>Equipment: {equipment_group} <br>Alarms: {alarms_hover}<br>Type: Active Alarm<br>Duration: {minutes_to_hhmm(round(downtime_total, 2))}<extra></extra>',
                        showlegend=False
                    ))
                last_time = hour

        if last_time < time_range[-1]:
            active_alarms = alarm_data[(alarm_data['tslast'] >= last_time) & (alarm_data['tslast'] <= time_range[-1])]
            downtime_total = active_alarms['time_difference_minutes'].sum()
            alarms_list = active_alarms['alarm'].unique()
            if downtime_total > 0:
                fig.add_trace(go.Bar(
                    y=[equipment_group],
                    x=[downtime_total],
                    width=bar_width,
                    orientation='h',
                    name='Active Alarm',
                    marker_color='red',
                    base=last_time.hour * 60 + last_time.minute,
                    hovertemplate=f'Start Time: {last_time.strftime("%Y-%m-%d %H:%M")} <br>Equipment: {equipment_group} <br>Alarms: {alarms_hover}<br>Type: Active Alarm<br>Duration: {minutes_to_hhmm(round(downtime_total, 2))}<extra></extra>',
                    showlegend=False
                ))

    fig.update_layout(
        barmode='stack',
        title=f"Downtime for {selected_date}",
        xaxis_title="Time (minutes)",
        yaxis_title="Equipment Group",
        yaxis=dict(title_standoff=10),
        height=600,
        xaxis=dict(range=[0, 60 * 24]),
        margin=dict(l=40, r=40, t=40, b=40),
        legend_title_text="Alarm Status"
    )

    return fig

@app.callback(
    Output('output-graph', 'children'),
    Output('loading-status', 'children'),
    Input('generate-figure-btn', 'n_clicks'),
    State('date-picker', 'date')
)
def update_graph(n_clicks, selected_date):
    if n_clicks > 0:
        loading_message = "Loading..."
        df = parse_frappe_api(selected_date)
        
        if isinstance(df, str):
            return html.Div(df), loading_message  # Return error message
        
        fig = create_figure(selected_date, df)
        
        return dcc.Graph(figure=fig), "Figure generated."
    return dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)
