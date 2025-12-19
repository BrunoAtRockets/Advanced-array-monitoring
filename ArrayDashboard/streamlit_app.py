
import streamlit as st                          #library used for webdevelopment
import pandas as pd                             #table library
import plotly.express as px                     #figure/plot library
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import math                                     #math
from pathlib import Path                    


import sqlite3                                  #sql library
from sqlalchemy import create_engine, text      #sql libray
import urllib                                   

from datetime import date, timedelta

#-------------------- runtime variables -----------------

date_format = '%Y-%m-%d' #date format from database

#sql server
driver = '{ODBC Driver 17 for SQL Server}'
server = 'localhost\MSSQLSERVER01'
database = 'R1ArrayMonitor'
username = 'R1_import_service'
password = 'service.py'


params = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}') # sql connection parameteres


#database tables
Inverter_data_tb = 'R1_Inverter_data' #stores Inverter FTP data
Producer_data_tb = 'R1_Producer_data' #stores Producer (DAQ and web scrapper) data

Inverter_data_column = 'Metric'     #type column from inverter db table
Producer_data_column = 'Variable'   #type column from producer db table


#inverter serial number to inverter number dictionary
inverter_mapping = {
    '2002082591' : 'Inverter 1',
    '2006601012' : 'Inverter 2',
    '2007325801' : 'Inverter 3',
    '2007325768' : 'Inverter 4'
}

#-------------------- Program queries ---------------------

#@st.cache_data
#decorator in the Streamlit library that is used to prevent the re-computation of data 
# and heavy functions every time a user interacts with your application (which causes a full Python script rerun).

@st.cache_data 
def get_unique_param(db_table, Column): 
    #queries all unique parameters from target table and target column
    try:
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}', pool_pre_ping=True) #creates sql engine
        with engine.connect() as connection: #create connection
            query = f"""
                    SELECT DISTINCT TOP 100 [{Column}] FROM {db_table} 
                    """
            df = pd.read_sql(query,con=connection) #perform query
            return df #return query result
    except Exception as e:
        print(e)

@st.cache_data
def sql_dataframe_import(db_table,metric_column, metrics_list, start_date, end_date):
    #perform query given a sql table, metric column, metric list, and start and end date
    try:    
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}', pool_pre_ping=True) #create engine
        with engine.connect() as connection: # create connection
            placeholders = ', '.join([f':metric_{i}' for i, _ in enumerate(metrics_list)]) #dynamically create a string of placeholders: '?,?,?....
            query_string = f"""
            SELECT *
            FROM {db_table}
            WHERE [{metric_column}] IN ({placeholders})
            AND [TimeStamp] BETWEEN :start_date AND :end_date
            """
            query = text(query_string) #wrap query string
            #make query parameters list
            params_query = {f'metric_{i}': metric for i, metric in enumerate(metrics_list)} #add metric params to params query 
            params_query['start_date'] = start_date #add start date to params query
            params_query['end_date'] = end_date #add end date to params query
            df = pd.read_sql_query(query,connection, params=params_query) #perform query
            return df #return query result
    except Exception as e:
        print(e)

def sql_producer_live_import():
    #live import query, no parameters needed
    #Selects all columns from R1_producer_data sql table between current time and 3 days ago
    try:
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}', pool_pre_ping=True) #create engine
        with engine.connect() as connection: #create connection
            query = """
            DECLARE @StartDate DATETIME = DATEADD(DAY, -3,GETDATE());
            SELECT * FROM R1_Producer_data
            WHERE [TimeStamp] >= @StartDate
            """
            df = pd.read_sql(query, connection) #perform query
            return df #return query result
    
    except Exception as e:
        print(e)

#@st.fragment(run_every='60s')
#decorator with a specific update frequency, which suggests you want a portion of your Streamlit app to run 
#and update automatically every 60 seconds, independent of full user interactions.
@st.fragment(run_every='60s')
def live_data_display(Producer_variables_list):
    #live data display is the function that queries and displays live data from R1_producer_data sql table
    #function is updated every minute
    Producer_data_df = sql_producer_live_import() #perform live data import query
    Producer_data_df = Producer_data_df.sort_values(by='TimeStamp', ascending=True)
    latest_timestamp = Producer_data_df['TimeStamp'].max() #get last timestamp
    latest_rows = Producer_data_df[Producer_data_df['TimeStamp'] == latest_timestamp]
    
    st.header(f'Performance Metric (1min interval): :blue[{latest_timestamp}]', divider='gray')
    
    cols = st.columns(3)

    for i, variable in enumerate(Producer_variables_list):
        col = cols[i % len(cols)]
        value_df = latest_rows.loc[latest_rows['Variable'] == variable, 'Magnitude']
        metric_magnitude = round(value_df.iloc[0],2)
        unit_df = latest_rows.loc[latest_rows['Variable'] == variable, 'Units']
        metric_unit = unit_df.iloc[0]
        #print(metric_value)
        #print(metric_unit)
        with col:

            st.metric(
                label= variable,
                value= f'{metric_magnitude}\t{metric_unit}',
                #delta=growth,
                #delta_color=delta_color
            )
    ''

    Producer_temperature_data_df = Producer_data_df[Producer_data_df['Variable'].isin(['Tmod', 'Tair'])]


    fig_Temperature = px.line(
        Producer_temperature_data_df,
        x='TimeStamp',
        y='Magnitude',
        color='Variable',
        #line_group='SerialNumber',
        title=f'Temperature ({Producer_temperature_data_df['Units'].iloc[0]})',
        labels={'Magnitude':f'Temperature [{Producer_temperature_data_df['Units'].iloc[0]}]'}
        #hover_data=['Units', 'Metric', 'Value']
    )
    st.plotly_chart(fig_Temperature, width='stretch')

    Producer_power_data_df = Producer_data_df[Producer_data_df['Variable'].isin(['Power'])]
    Producer_POA_data_df = Producer_data_df[Producer_data_df['Variable'].isin(['POA'])]
    Producer_POA2_data_df = Producer_data_df[Producer_data_df['Variable'].isin(['POA2'])]
    Producer_ALB_data_df = Producer_data_df[Producer_data_df['Variable'].isin(['ALB'])]

    fig_power_irradiance = make_subplots(specs=[[{"secondary_y" : True}]])

    fig_power_irradiance.add_trace(
        go.Scatter(
            x=Producer_power_data_df['TimeStamp'],
            y=Producer_power_data_df['Magnitude'],
            name=f'Power [{Producer_power_data_df['Units'].iloc[0]}]',
            mode='lines+markers'
        ),
        secondary_y=False
    )
    fig_power_irradiance.add_trace(
        go.Scatter(
            x=Producer_POA_data_df['TimeStamp'],
            y=Producer_POA_data_df['Magnitude'],
            name=f'POA [{Producer_POA_data_df['Units'].iloc[0]}]',
            mode='lines+markers'
        ),
        secondary_y=True
    )
    fig_power_irradiance.add_trace(
        go.Scatter(
            x=Producer_POA2_data_df['TimeStamp'],
            y=Producer_POA2_data_df['Magnitude'],
            name=f'POA2 [{Producer_POA2_data_df['Units'].iloc[0]}]',
            mode='lines+markers'
        ),
        secondary_y=True
    )
    fig_power_irradiance.add_trace(
        go.Scatter(
            x=Producer_ALB_data_df['TimeStamp'],
            y=Producer_ALB_data_df['Magnitude'],
            name=f'ALB [{Producer_ALB_data_df['Units'].iloc[0]}]',
            mode='lines+markers'
        ),
        secondary_y=True
    )

    fig_power_irradiance.update_layout(title_text='Power and Sensor irradiance data')
    fig_power_irradiance.update_xaxes(title_text='TimeStamp')
    fig_power_irradiance.update_yaxes(title_text="<b>Power</b>", secondary_y=False)
    fig_power_irradiance.update_yaxes(title_text="<b>Irradiance</b>", secondary_y=True)
    st.plotly_chart(fig_power_irradiance, width='stretch')

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='R1 PV Dashboard',
    layout='wide',
    page_icon=':sunny:', # This is an emoji shortcode. Could be a URL too.
)

# -----------------------------------------------------------------------------

Inverter_metrics_df = get_unique_param(Inverter_data_tb,'Metric')

Producer_variables_df = get_unique_param(Producer_data_tb,'Variable')



Inverter_metrics = Inverter_metrics_df['Metric']
Met_metrics = Producer_variables_df[Producer_variables_df['Variable'].isin(['POA', 'POA2', 'GHI', 'ALB', 'Tair', 'Tmod'])]

# -----------------------------------------------------------------------------
# Draw the actual page

# Set the title that appears at the top of the page.
'''
# :sun_with_face: University of Toledo Research Building Array Dashboard

This dashboard was designed to display the energy and environmental impact of the research building
photovoltaic array. 


'''

''
Producer_variables_list = ['Power', 'DailyYield', 'TotalYield', 'GHI', 'POA', 'POA2', 'ALB', 'Tair', 'Tmod']



live_data_display(Producer_variables_list)

''
#date range widget
today = date.today()

if today.weekday() == 0:
    default_start_date = today - timedelta(days=7)
else:
    default_start_date = today - timedelta(days=today.weekday())



#display the default date range value as a tuble
default_date_range = (default_start_date, today)


selected_dates = st.date_input(
    'Select a date range',
    value=default_date_range,
    key='date_range_input'
)
#check if input is a tuple of length 2
if selected_dates and len(selected_dates) == 2:
    #if true tuple is safe
    from_date, to_date = selected_dates
    st.success(f'A full date range is selected: {from_date.strftime(date_format)} to {to_date.strftime(date_format)}')

else: 
    st.warning('Please select a date range')
    from_date = default_start_date
    to_date = today

from_date = from_date.strftime(date_format)
to_date = to_date + timedelta(days=1)
to_date = to_date.strftime(date_format)

# Add some spacing
''


multiselect_cols = st.columns(2)
with multiselect_cols[0]:
    if not(len(Inverter_metrics)):
        st.warning("Select at least one Inverter Metric")
    selected_inverter_metrics = st.multiselect(
        'Which inverter metric would you like to view?',
        Inverter_metrics,
        ['E-Total']
    )


with multiselect_cols[1]:
    if not(len(Met_metrics)):
        st.warning("Select at least one Metereology metric")
    selected_met_variables = st.multiselect(
        'Which Metereology metric would you like to view',
        Met_metrics,
        ['POA']
    )
    

st.header('Inverter Data', divider='gray')

Inverter_data_df = sql_dataframe_import(Inverter_data_tb,Inverter_data_column,selected_inverter_metrics,from_date,to_date)
Inverter_data_df = Inverter_data_df.sort_values(by='TimeStamp', ascending=True)
Inverter_data_df['SerialNumber'] = Inverter_data_df['SerialNumber'].astype(str)
Inverter_data_df['Inverter Number'] = Inverter_data_df['SerialNumber'].map(inverter_mapping)
Serial_numbers_list = list(inverter_mapping.keys())

Met_data_df = sql_dataframe_import(Producer_data_tb,Producer_data_column, selected_met_variables, from_date, to_date)
Met_data_df = Met_data_df.sort_values(by='TimeStamp', ascending=True)

cols = st.columns(len(Serial_numbers_list))

for i, metric in enumerate(selected_inverter_metrics):
    #Filter for just specific metric
    single_metric_df = Inverter_data_df[Inverter_data_df['Metric'] == metric]

    for j, SerialNumber in enumerate(Serial_numbers_list):

        single_SerialNumber_df = single_metric_df[single_metric_df['SerialNumber'] == SerialNumber]

        
        fig = px.scatter(
            single_SerialNumber_df,
            x='TimeStamp',
            y='Mean',
            color='Inverter Number',
            #line_group='SerialNumber',
            title=f'{metric} ({single_SerialNumber_df['Units'].iloc[0]}) vs Time',
            #hover_data=['Units', 'Metric', 'Value']
        )
        fig.update_layout(
            yaxis_title_text = f'{metric} ({single_SerialNumber_df['Units'].iloc[0]})'
        )
        with cols[j]:
            st.plotly_chart(fig, width='stretch')



st.header('Metereology data', divider='gray')
met_variable_df = Met_data_df[Met_data_df['Variable'].isin(selected_met_variables)]


Met_fig = px.scatter(
    met_variable_df,
    x='TimeStamp',
    y='Magnitude',
    color='Variable',
    title='Metereology over time'
)
st.plotly_chart(Met_fig,width='stretch')   

#-------------------------------------------
