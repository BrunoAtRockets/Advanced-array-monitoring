# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#-------------------------- import libraries -----------------------------------------
import os 
import io
from pathlib import Path


import time
from datetime import datetime, date

import nidaqmx
from nidaqmx.constants import AcquisitionType, READ_ALL_AVAILABLE, TerminalConfiguration



import pandas as pd
import numpy as np
import math

import multiprocessing


import requests


from sqlalchemy import create_engine, text
import urllib


from ftplib import FTP

import zipfile
import xml.etree.ElementTree as ET


import plotly.graph_objects as go
from plotly.subplots import make_subplots


import win32com.client as win32


import gc





# ----------------Runtime constants/global variables ------------------ 
#DAQ channel definition
DEV_TAIR = "Dev1/ai0"
DEV_TMOD = "Dev1/ai4"
DEV_POA = "Dev1/ai2"
DEV_POA2 = "Dev1/ai6"
DEV_ALB = "Dev1/ai3"
DEV_GHI = "Dev1/ai1"

#Array positions
C_TMOD = 0
C_TAIR = 1
C_POA = 2
C_POA2 = 3
C_ALB = 4
C_GHI = 5

#DAQ sendor units
Tmod_unit = 'C'
Tair_unit = 'C'
POA_unit = 'W/m2'
POA2_unit = 'W/m2'
GHI_unit = 'W/m2'
ALB_unit = 'W/m2'


#Sunny Webbox login page
url = "http://r1array.ed.utoledo.edu/home.ajax"

#unit mapping for Inv data 
Inv_data_mapping = {
    'CO2 saved': 'lbs',
    'E-Total': 'kWh',
    'Fac': 'Hz',
    'Inv.TmpVal': 'C',
    'Pac': 'W',
    'Pcb.TmpVal': 'C',
    'Vac': 'V',
    'VacL1': 'V',
    'VacL2': 'V',
    'Vpv': 'V',
    'Max Vpv': 'V',
    'Vpv-Setpoint': 'V',
    'Iac': 'A',
    'Ipv': 'A',
    'h-On': 'h',
    'h-Total': 'h',
    'Riso': 'kOhm',  # Assuming Riso is in kilo-ohms
    'Power On': '',
    'Event-Cnt': '',
    'I-dif': 'mA',
    'Mode': '',
    'Backup State': '',
    'Balancer': '',
    'Error': '',
    'Serial Number': '',
    'Grid Type': '',
    'Temperature' : 'C',
    'Vfan' : 'V'
    }


#DAQ ouput voltage
DAQ_output_voltage = 2.5
Vex = 2.5 #R_temp excitation voltage = to DAQ output voltage

#Sensitivity factor to calculate W/m2 from V 
#change according to calibration specifications
POA_SENSITIVITY = 0.00001450
ALB_SENSITIVITY = 0.00001312
GHI_SENSITIVITY = 0.00001476



#pyranometers offset
#df_offset = pd.read_csv('C:/Users/phili/OneDrive/Documents/R1 Array Data/Pyranometer_Offset.csv') #load from .csv save file
#df_offset['Offset'] = df_offset['Offset'].astype(float)
#assign offset to global variables
#POA_off = df_offset.loc[df_offset['Variable'] == 'POA_off', 'Offset'].iloc[0]
#GHI_off = df_offset.loc[df_offset['Variable'] == 'GHI_off', 'Offset'].iloc[0]
#ALB_off = df_offset.loc[df_offset['Variable'] == 'ALB_off', 'Offset'].iloc[0]

#pyranometers offset variables
POA_off = 0
POA2_off = 0
GHI_off = 0
ALB_off = 0

#Temp math error flag
Tmod_flag = 0
Tair_flag = 0

#DAQ configuration
sampling_rate = 1000  # Samples per second
samples_to_acquire = 10  # Number of samples to acquire

#redundancy data
producer_dir = 'C:/Users/phili/OneDrive/Documents/R1 Array Data/Producer_OUT/'


#FTP server variables
FTP_HOST = 'astro1.panet.utoledo.edu'   #host
FTP_USER = 'rjer1data'                  #user 
FTP_PASSWORD = 'AdamRoan$q'             #password



local_root = 'C:/Users/phili/OneDrive/Documents/R1 Array Data/FTP_IN'   #FTP target folder
pyra_offset_file = 'C:/Users/phili/OneDrive/Documents/R1 Array Data/Pyranometer_offsets.csv' #pyranometer offset daily csv
historical_offset_file = 'C:/Users/phili/OneDrive/Documents/R1 Array Data/Historical_Pyranometer_offsets.csv' #historical pyranometer offsets

#sql server credentials
driver = '{ODBC Driver 17 for SQL Server}'
server = r'localhost\MSSQLSERVER01'
database = 'R1ArrayMonitor'
username = 'R1_import_service'
password = 'service.py'
params = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

#database tables
Inverter_data_tb = 'R1_Inverter_data' #collects Inverter FTP data
Producer_data_tb = 'R1_Producer_data' #collects Producer (DAQ and web scrapper) data


#inverter serial number to inverter number dictionary
inverter_mapping = {
    '2002082591' : 'Inverter 1',
    '2006601012' : 'Inverter 2',
    '2007325801' : 'Inverter 3',
    '2007325768' : 'Inverter 4'
}

#------------------------------- Function definitions ---------------------------------------------------------------------------------------
#these are the functions used during runtime

#download ftp download/delete all files from FTP server onto target folder
def ftp_download():
    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASSWORD) #login to FTP server
            ftp.cwd('R1InverterData/') #move to R1InverterData folder
            zip_files = ftp.nlst() #get list of files
            for file in zip_files: 
                local_filepath = os.path.join(local_root, file)
                with open(local_filepath, 'wb') as local_file:
                   ftp.retrbinary(f'RETR {file}', local_file.write) #save to targed folder
                   ftp.delete(file) #delete FTP file
    except Exception as e:
        print(f"An error occurred: {e}")
        

#convert ftp xml files to pandas dataframes return pandas dataframe
#This function is called by Inv_data_formatting
def xml_to_dataframe(xml_data_bytes):
    root = ET.fromstring(xml_data_bytes) #parse xml string
    data = []
    for item in root.findall('.//MeanPublic'): #find all items in MeanPublic
        full_key = item.find('Key').text            
        parts = full_key.split(':')
        serial_number = parts[1] 
        metric = parts[2]
        #create row for data table
        row = {
            'Serial Number': int(serial_number),
            'Metric':   metric,
            'Mean':     float(item.find('Mean').text),
            'Base':     int(item.find('Base').text),
            'Period':   int(item.find('Period').text),
            'TimeStamp': item.find('TimeStamp').text
            }
        data.append(row) #append rows
    df = pd.DataFrame(data) #create dataframe with xml data
    df['Units'] = df['Metric'].map(Inv_data_mapping)
    return df

#After downloading FTP files from FTP server. This function loads every file downloaded
#and formats them into a data frame to be imported to db and saved into a csv
#inside each zip folder contains many other zip folders with 'Log' folders and 'Mean' folders,
#inside each 'Mean' folder there's a Mean XML file that has all incomming data from the 4 inverters connected to the webbox
def Inv_data_formatting():
    
    df_FTP_list = [] #empty list that stores each data frame created by this function
    inv_zip_list = os.listdir(local_root) #list of all the FTP downloaded files
    #print(inv_zip_list)
    for zip_folder in inv_zip_list: 
        local_filepath = os.path.join(local_root, zip_folder) 
        with zipfile.ZipFile(local_filepath, 'r') as zf: #open zipfolder
            inner_zip_list = zf.namelist() #list of files inside zipfolder
            for inner_filename in inner_zip_list:
                if inner_filename.startswith('Mean.') and inner_filename.endswith('.zip'): #make sure you open a folder with the Mean inverter data
                    #print(f'found nested zip: {inner_filename}')
                    inner_zip_bytes = zf.read(inner_filename) #read inner zip folder
                    with zipfile.ZipFile(io.BytesIO(inner_zip_bytes)) as nested_zip: #load data inside inner zipfolder
                        xml_files_list = nested_zip.namelist() #list of xml files
                        for xml_file in xml_files_list:
                            xml_data_bytes = nested_zip.read(xml_file) #read xml file
                            df = xml_to_dataframe(xml_data_bytes) #send xml file to xml_to_dataframe fundtion
                            df_FTP_list.append(df) #append created dataframe to list of dataframes
        #delete FTP zip files in FTP_IN folder
        if os.path.exists(local_filepath):
            #print(f'{local_filepath} has been found')
            try:
                os.remove(local_filepath) #delete zip
                #print(f'file {local_filepath} deleted')
            except OSError as e:
                print(e)
        else:
            print('file not found')
    
    #concat all dataframes to a single dataframe    
    df_FTP = pd.concat(df_FTP_list, ignore_index=True)    
    df_FTP['TimeStamp'] = pd.to_datetime(df_FTP['TimeStamp']) #convert TimeStamp column to datetime format
    df_FTP['TimeStamp'] = df_FTP['TimeStamp'].dt.strftime('%Y/%m/%d %H:%M:%S') #change format of TimeStamp coulumn
    
    df_FTP = df_FTP.rename(columns={'Serial Number':'SerialNumber'}) #rename SerialNumber column to remove space
    
    #specify columns to filter
    filter_metrics = ['Backup State',
                      'Balancer',
                      'Error',
                      'Event-Cnt',
                      'Grid Type',
                      'Mode',
                      'Power On',
                      'Serial Number',
                      'Max Temperature',
                      'Max Vpv',
                      'Vpv-_PE',
                      'Vfan',
                      'Temperature'
                      ]
    
    
    df_FTP_filtered = df_FTP[~df_FTP['Metric'].isin(filter_metrics)] #filter specified columns
    
    
    #get first and last timestamp to name the output .csv file
    df_FTP_timestamp_column = df_FTP['TimeStamp'] 
    TSfrom = datetime.strptime(df_FTP_timestamp_column.iloc[0], '%Y/%m/%d %H:%M:%S')
    TSto = datetime.strptime(df_FTP_timestamp_column.iloc[-1], '%Y/%m/%d %H:%M:%S')
    
    TSfrom = TSfrom.strftime('%Y%m%d_%H%M%S')   #from TimeStamp
    TSto = TSto.strftime('%Y%m%d_%H%M%S')       #to TimeStamp  
    
    df_FTP.to_csv(f'C:/Users/phili/OneDrive/Documents/R1 Array Data/FTP_OUT/FTP_from_{TSfrom}_to_{TSto}.csv', index=False) #Save concatenated and unfiltered dataframe to .csv
    
    #return concatenated and formatted inverter data 
    return df_FTP_filtered



#Function that takes a dataframe and target table as input and exports the data to the target SQL table database
def sql_dataframe_export(import_df,target_table):
    try:
        with create_engine(f'mssql+pyodbc:///?odbc_connect={params}', pool_pre_ping=True).connect() as conn:
            import_df.to_sql(target_table,con=conn,if_exists='append',index=False)
    except Exception as e:
        print(e)



def sql_producer_live_import(db_table, days_to_collect):
    #live import query, no parameters needed
    #Selects all columns from R1_producer_data sql table between current time and 3 days ago
    try:
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}', pool_pre_ping=True) #create engine
        with engine.connect() as connection: #create connection
            query = f"""
            DECLARE @StartDate DATETIME = CAST(DATEADD(DAY, :days_ago_param ,GETDATE()) AS DATE);
            SELECT * FROM {db_table}
            WHERE [TimeStamp] >= @StartDate
            """
            query = text(query)
            params_dict = {'days_ago_param' : days_to_collect}
            df = pd.read_sql(query, connection, params=params_dict) #perform query
            return df #return query result
    
    except Exception as e:
        print(e)


#sends an email letting the owner know this program is running
def send_daily_notification():
    global Tair_flag
    global Tmod_flag
    
    #email message
    html_body = f'''
    <html>
        <body>
            <p>Hello,</p>
            <p>R1 CdTe PV Array is running.</p>
            <p>Tair flag: {Tair_flag}<p>
            <p>Tmod flag: {Tmod_flag}<p>
            <p>Regards,</p>
            <p>R1ArrayMonitor</p>
        </body>
    </html>
    '''
    try:
        #send email
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = 'bhinojo2@rockets.utoledo.edu'
        mail.Subject = 'R1 Array Daily Notification'
        mail.HTMLBody = html_body
        mail.Send()
    except Exception as e:
        print (e)

#weekly summary email
def send_email(db_table, days_to_collect): #table to query and days to collect
    try:
        Inverter_data_df = sql_producer_live_import(Inverter_data_tb, -7) #get data from the database
        Inverter_data_df = Inverter_data_df.sort_values(by='TimeStamp', ascending=True) #sort values by time
        Inverter_data_df['SerialNumber'] = Inverter_data_df['SerialNumber'].astype(str) 
        Inverter_data_df['Inverter'] = Inverter_data_df['SerialNumber'].map(inverter_mapping) #make inverter column according to serial number
        
        Inverter_power_filtered = Inverter_data_df[Inverter_data_df['Metric'] == 'Pac'] #filter all metrics but Pac
        Inverter_power_filtered['Date'] = Inverter_power_filtered['TimeStamp'].dt.date #make date column
        
        #Etotal for all inverters
        Etotal_df = Inverter_data_df[Inverter_data_df['Metric'] == 'E-Total'] 
        Most_recent_Etotal_df = Etotal_df.groupby('Inverter').first().reset_index() #get most recent Etotal
        Etotal = Most_recent_Etotal_df['Mean'].sum().round(2) #sum all inverters Etotal
        
        #weekly kmh per inverter calculation
        weekly_kwh_per_inverter = Inverter_power_filtered.groupby(['Inverter'])['Mean'].sum().reset_index() #sum all Pac mean values for the week
        weekly_kwh_per_inverter['Mean'] = (weekly_kwh_per_inverter['Mean']/(1000 * 60)).round(2) #calculate kwh
        weekly_kwh_per_inverter['Weekly Total Energy Generated'] = weekly_kwh_per_inverter['Mean'].astype(str) + ' kWh' #make string with energy and units
        
        weekly_total_kwh = weekly_kwh_per_inverter['Mean'].sum().round(2) #calculate weekly total kwh
        
        
        weekly_kwh_per_inverter = weekly_kwh_per_inverter.drop(columns=['Mean']) #filter dataframe for email display
        weekly_kwh_per_inverter_html = weekly_kwh_per_inverter.to_html(index=False, border=1, classes= 'dataframe') #create a html string from dataframe
        
        Inverter_sum = Inverter_power_filtered.groupby('TimeStamp')['Mean'].sum().reset_index() #weekly power per inverter
        
        daily_kwh = Inverter_sum.groupby(Inverter_sum['TimeStamp'].dt.date)['Mean'].sum().reset_index() #daily total kwh
        
        daily_kwh['Mean'] = (daily_kwh['Mean'] * 5 / (1000*60)).round(2) #convert to kwh
        daily_kwh['Daily Yield'] = daily_kwh['Mean'].astype(str) + ' kWh' # add units
        daily_kwh['Afternoon'] = pd.to_datetime(daily_kwh['TimeStamp']) + pd.to_timedelta(12, unit='h') #make afternoon timestamped column
        
        daily_kwh_filtered = daily_kwh.drop(columns=['Mean', 'Afternoon']) #filter for eamil display
        daily_kwh_filtered_html = daily_kwh_filtered.to_html(index=False, border=1, classes='dataframe') # create html string for email display
        
        #min and max dates
        min_date = Inverter_data_df['TimeStamp'].min().date()
        max_date = Inverter_data_df['TimeStamp'].min().date()
        
        #max power
        max_power = Inverter_sum['Mean'].max()
        daily_kwh['Max_Power'] = max_power #create max power column
        
        fig_power = make_subplots(specs=[[{"secondary_y" : False}]])
        
        #create power graph
        fig_power.add_trace(
            go.Scatter(
                x=Inverter_sum['TimeStamp'],
                y=Inverter_sum['Mean'],
                name='Power (W)',
                mode='lines',
                line=dict(color='blue', width=2)
            ),
            secondary_y=False
        )
        #create daily yield production labels
        fig_power.add_trace(
            go.Scatter(
                x=daily_kwh['Afternoon'],
                y=daily_kwh['Max_Power'],
                name='R1 CdTe PV Array Weekly Report',
                mode='text',
                textposition='top center',
                marker=dict(color='blue', size=10),
                text=daily_kwh['Daily Yield'],
                showlegend=False
            ),
            secondary_y=False
        )      
        #figure congig
        fig_power.update_layout(title_text='R1 CdTe PV Array Weekly Report', title_x=0.5)
        fig_power.update_xaxes(title_text='TimeStamp',showgrid=True)
        fig_power.update_yaxes(title_text="<b>Power (W)</b>", showgrid=True)
        
        #save image
        folder_name = r'C:/Users/phili/OneDrive/Documents/R1 Array Data/Email Attachments'
        fig_power_name = 'Power.png'
        power_fig_path = Path(folder_name)/fig_power_name
        fig_power.write_image(str(power_fig_path))
        
        #delete figure from runtime memory
        del fig_power
        gc.collect()
        
        #email body
        html_body = f'''
        <html>
            <body>
                <p>Hello,</p>
                <p>R1 CdTe PV Array Weekly Report for {min_date} to {max_date}.</p>
                <p>Total Energy Generated this week: {weekly_total_kwh} kWh</p>
                <p>Cumulative Total Energy is: {Etotal} 
                <h3>Total Weekly Energy Generated per Inverter:</h3>
                {weekly_kwh_per_inverter_html}
                <h3>Daily Power Generation:</h3>
                {daily_kwh_filtered_html}
        
                <p>Please find the attached power generation figure for the week.</p>
                <p>Regards,</p>
                <p>R1ArrayMonitor</p>
            </body>
        </html>
        '''
        #send email
        try:
            outlook = win32.Dispatch('outlook.application')
            mail = outlook.CreateItem(0)
            recipients = ['bhinojo2@rockets.utoledo.edu', 'Randy.Ellingson@utoledo.edu', 'toozie@yahoo.com', 'randy@glasscitycommunitysolar.org'] #recipients
            mail.To = ";".join(recipients)
            mail.Subject = 'R1 Array weekly update'
            mail.HTMLBody = html_body
            
            current_dir = os.path.dirname(power_fig_path)
            attachedment_file = os.path.join(current_dir, fig_power_name)
            print(attachedment_file)
            
            if os.path.exists(attachedment_file):
                mail.Attachments.Add(attachedment_file)
            mail.Send()
            print('email sent')
        
        except Exception as e:
            print(f' exception: {e}')
        
    except Exception as e:
        print(e)
        
    
    

#FTP worker combines all FTP download, formatting and uploading function to be the target of the multiprocessing thread
#of this program. This allows the program to export inverter data to database without interrupting the data collection main thread
#and sends an email notification that the program is running
def FTP_worker(target_table):
    ftp_download()
    export_df = Inv_data_formatting()
    sql_dataframe_export(export_df, target_table)
    send_daily_notification()
        



#Function to set DAQ ouput voltage to DAQ_out_voltage value
def daq_Vout(DAQ_output_voltage):
    with nidaqmx.Task() as ao_task:
        ao_task.ao_channels.add_ao_voltage_chan('Dev1/ao0')
        ao_task.write(DAQ_output_voltage)
        ao_task.start()


#Function responsible to scrape data from the Sunny Webbox page where the power produced by the R1 building array is displayed
#takes as input the url to be scrapped and the TimeStamp of the data scrapping
def web_producer(url):
    try:
        with requests.Session() as session:
            
            response = requests.get(url) #request url response
            producer_output_magnitude = []
            producer_output_unit = []
            
            if response.status_code == 200: #if response is successful
                data = response.json() #load url data
                
                
                #Plant power data is under 'Items'
                magnitude, unit = data['Items'][0]['Power'].split() #get Power magnitude and unit from url data
                producer_output_magnitude.append(float(magnitude)) #convert string to float and append it to magnitude array
                producer_output_unit.append(unit) #Power append unit to unit array
                
                #do the same for DailyYield....
                magnitude, unit = data['Items'][1]['DailyYield'].split()
                producer_output_magnitude.append(float(magnitude))
                producer_output_unit.append(unit)
                
                
                #and do the same for TotalYied....
                magnitude, unit = data['Items'][2]['TotalYield'].split()
                producer_output_magnitude.append(float(magnitude))
                producer_output_unit.append(unit)
                
                
                #Create a data frame with scrapped web data
                web_producer_data = pd.DataFrame(
                    {#'TimeStamp': [timestamp,timestamp,timestamp],
                     'Variable':['Power', 'DailyYield', 'TotalYield'],
                     'Magnitude':[producer_output_magnitude[0],producer_output_magnitude[1],producer_output_magnitude[2]],
                     'Units':[producer_output_unit[0],producer_output_unit[1],producer_output_unit[2]]
                        })
                #web_producer_data['TimeStamp'] = pd.to_datetime(web_producer_data['TimeStamp']) #add TimeStamp
                #return dataframe
                return web_producer_data
    
            else: #if url connection is not successful then return a dataframe with empty values
                web_producer_data = pd.DataFrame(
                    {#'TimeStamp': [timestamp],
                     'Variable':['Power'],
                     'Magnitude':[np.nan],
                     'Units':[""]
                        })
                
                return web_producer_data
    except:
        web_producer_data = pd.DataFrame(
            {#'TimeStamp': [timestamp],
             'Variable':['Power'],
             'Magnitude':[np.nan],
             'Units':[""]
                })
        return web_producer_data



#DAQ Producer function that takes timestamp as an input
def daq_producer(POA_offset = 0,POA2_offset = 0, GHI_offset = 0, ALB_offset = 0):
    DAQ_data_compressed = []
    
    global Tair_flag
    global Tmod_flag
    
    try:
        with nidaqmx.Task() as ai_task: #create a DAQ input task
            # Add an analog input voltage channels
            
            #Module temperature channel config
            ai_task.ai_channels.add_ai_voltage_chan(DEV_TMOD,
                                                    terminal_config=TerminalConfiguration.RSE,
                                                    min_val=0,
                                                    max_val=1)  #channel 0
            
            #Air temperature channel config
            ai_task.ai_channels.add_ai_voltage_chan(DEV_TAIR,
                                                    terminal_config=TerminalConfiguration.RSE,
                                                    min_val=0,
                                                    max_val=1)  #channel 4
            #POA
            #ai_task.ai_channels.add_ai_voltage_chan(DEV_POA)   #channel 2
            ai_task.ai_channels.add_ai_voltage_chan(DEV_POA,
                                                    terminal_config=TerminalConfiguration.RSE,
                                                    min_val=0,
                                                    max_val=1)
            ai_task.ai_channels.add_ai_voltage_chan(DEV_POA2,
                                                    terminal_config=TerminalConfiguration.RSE,
                                                    min_val=0,
                                                    max_val=1)
            
            #ALB
            ai_task.ai_channels.add_ai_voltage_chan(DEV_ALB)   #channel 1
            #GHI
            ai_task.ai_channels.add_ai_voltage_chan(DEV_GHI)   #channel 3
            
            
            # Configure timing for finite acquisition
            ai_task.timing.cfg_samp_clk_timing(
                sampling_rate, 
                sample_mode=AcquisitionType.FINITE, 
                samps_per_chan=samples_to_acquire
            )
            
    
            
            # Read the acquired data
            DAQ_data = ai_task.read(READ_ALL_AVAILABLE)
            DAQ_data_compressed = [sum(sensor)/len(sensor) for sensor in DAQ_data]
            #print(DAQ_data_compressed)
            
            #Calculations
            try: 
                R_Tmod = (26990 + 24)/((Vex/DAQ_data_compressed[C_TMOD]) - 1) #R_Tmod Calculation
                DAQ_data_compressed[C_TMOD] = 1/((9.376e-4) + (2.208e-4)*math.log(R_Tmod) + (1.276e-7)*math.log(R_Tmod)**3) - 273.15 #Tmod calculation
            except:
                Tmod_flag = 1
                DAQ_data_compressed[C_TMOD] = 0
            try:
                R_Tair = (26990 + 24)/((Vex/DAQ_data_compressed[C_TAIR]) - 1) #R_Tair Calculation
                DAQ_data_compressed[C_TAIR] = 1/((9.376e-4) + (2.208e-4)*math.log(R_Tair) + (1.276e-7)*math.log(R_Tair)**3) - 273.15 #Tmod calculation
            except:
                Tair_flag = 1
                DAQ_data_compressed = 0
                
            DAQ_data_compressed[C_POA] = DAQ_data_compressed[C_POA]/0.00001450 + POA_offset #POA calculation
            DAQ_data_compressed[C_POA2] = DAQ_data_compressed[C_POA2]/0.00001134 + POA2_offset #POA2 calculation
            DAQ_data_compressed[C_ALB] = DAQ_data_compressed[C_ALB]/0.00001312 + ALB_offset #ALB calculation
            DAQ_data_compressed[C_GHI] = DAQ_data_compressed[C_GHI]/0.00001476 + GHI_offset #GHI calculation
            
            #create dataframe with collected data
            daq_producer_data = pd.DataFrame(
                {#'TimeStamp': [timestamp,timestamp,timestamp,timestamp,timestamp],
                 'Variable':['Tmod', 'Tair', 'POA', 'POA2', 'GHI', 'ALB'],
                 'Magnitude':[DAQ_data_compressed[C_TMOD],
                              DAQ_data_compressed[C_TAIR],
                              DAQ_data_compressed[C_POA],
                              DAQ_data_compressed[C_POA2],
                              DAQ_data_compressed[C_GHI],
                              DAQ_data_compressed[C_ALB]],
                 'Units':[Tmod_unit,Tair_unit,POA_unit,POA2_unit,GHI_unit,ALB_unit]
                    })
            #daq_producer_data['TimeStamp'] = pd.to_datetime(daq_producer_data['TimeStamp']) #add TimeStamp
            
            return daq_producer_data
    #if connection with DAQ faild return a dataframe with empty values
    except nidaqmx.DaqError as e: 
        daq_producer_data = pd.DataFrame(
            {#'TimeStamp': [timestamp,timestamp,timestamp,timestamp,timestamp],
             'Variable':['Tmod', 'Tair', 'POA', 'POA2', 'GHI', 'ALB'],
             'Magnitude':[np.nan,np.nan,np.nan,np.nan,np.nan,np.nan],
             'Units':['','','','','','']
                })
        return daq_producer_data
        


#------------------------- Main ---------------------------------------

#this function is the main logic of the program
#it is responsible to maintain timing, collect and save data
#every second the producer functions are called to collect data from the DAQ and the Webbox website
#every minute the data is averaged for measurements like irradiance and temperature or we get the max value for measurements like DailyYield
#the resuld of the avg/max is saved locally or sent to local database
#once a day the FTP function is called to import the FTP data to local db
#once a day a new pyranometer offset is calculated at 3am
    
def main():
    try:
        daq_Vout(DAQ_output_voltage=Vex) #apply output bias for thermocouple readings
        

        current_hour = datetime.now().hour #variable that tracks current hour
        
        #timing variables
        last_day = datetime.now().day
        last_minute = datetime.now().minute
        last_acquisition_time = time.time()
        
        
        data_from_producers_list = [] #array responsible to store data from producers
        
        #use global offset variables
        global POA_off
        global POA2_off
        global GHI_off
        global ALB_off
        
        
        
        offset_datatypes = { 'POA' : 'float64',
                            'POA2' : 'float64',
                            'GHI' : 'float64',
                            'ALB' : 'float64'
            }
        
        offset_df = pd.read_csv(pyra_offset_file, index_col=False)
        POA_off, POA2_off, GHI_off, ALB_off = offset_df.iloc[0]
        

        
        
        #while true
        while current_hour != 25: 
            
            time.sleep(0.1)
            timestamp = datetime.now()
            current_day = timestamp.day #tracks current day
            current_hour = timestamp.hour # tracks current hour
            current_minute = datetime.now().minute #current minute
            current_time = time.time() #current time
            
            #if 1 second has passed
            if current_time - last_acquisition_time >= 1:
                
                #Producer functions are called
                web_output = web_producer(url) 
                daq_output = daq_producer(POA_offset=POA_off, POA2_offset=POA2_off,ALB_offset=ALB_off,GHI_offset=GHI_off)
                
                #join data from both producers
                data_from_producers = pd.concat([web_output,daq_output], ignore_index=True) #concatenate data from both producers
                data_from_producers_list.append(data_from_producers) #add data collected to list 

                
                
                last_acquisition_time = current_time #update last_acquisition_time
                current_minute = datetime.now().minute #update current_minute every second
                
                #if a minute has passed
                if current_minute != last_minute:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') #get timestamp and assign following format 2005-12-31 16:20:01
                    df_producers_concat = pd.concat(data_from_producers_list, ignore_index=True) #create a new data frame from list of 1-sec data from producers
                    
                    avg_vars = ['Power', 'Tmod','Tair', 'POA', 'POA2', 'GHI', 'ALB'] #Variables to take the average of
                    max_vars = ['DailyYield', 'TotalYield'] #Variables to take the max value of
                    
                    
                    avg_df = df_producers_concat[df_producers_concat['Variable'].isin(avg_vars)].groupby('Variable').mean(numeric_only=True).reset_index() #get mean of avg variables
                    max_df = df_producers_concat[df_producers_concat['Variable'].isin(max_vars)].groupby('Variable').max().reset_index() #get max of max variables
                    units_df = df_producers_concat.groupby('Variable')['Units'].first().reset_index() #collect units 
                    
                    df_producers_min = pd.concat([avg_df, max_df]) #concatenate avg and max dataframes
                    df_producers_min = df_producers_min.drop(['Units'], axis=1) #drop units from concatenated avg/max
                    df_producers_min = df_producers_min.merge(units_df, on='Variable', how='left') #add units back
                    df_producers_min['TimeStamp'] = timestamp #add timestamp
                    df_producers_min['TimeStamp'] = pd.to_datetime(df_producers_min['TimeStamp'], format='%Y-%m-%d %H:%M:%S') #make timestamp column into a datetime column
                    
    
                    
                    
                    #save data
                    sql_dataframe_export(df_producers_min, Producer_data_tb) #send data to database
                    
                    
                    producer_file_path = producer_dir + date.today().strftime('%d%m%Y') + '.csv' #create filepath of the .csv file
                    df_producers_min.to_csv(producer_file_path, mode='a', header= not os.path.exists(producer_file_path), index=False) #save dataframe to .csv
                    
                    
                    
                    data_from_producers_list = []   #reset data from producers list
                    last_minute = current_minute #update last minute
                    
                    
                    #update offsets when it's 3am
                    if datetime.now().hour == 3 and datetime.now().minute == 00:
                     
                        
                        POA_off = POA_off - df_producers_min.loc[df_producers_min['Variable'] == 'POA', 'Magnitude'].iloc[0] 
                        POA2_off = POA2_off - df_producers_min.loc[df_producers_min['Variable'] == 'POA2', 'Magnitude'].iloc[0]
                        GHI_off = GHI_off - df_producers_min.loc[df_producers_min['Variable'] == 'GHI', 'Magnitude'].iloc[0]
                        ALB_off = ALB_off - df_producers_min.loc[df_producers_min['Variable'] == 'ALB', 'Magnitude'].iloc[0]
                        
                        offset_data = {
                            'POA': [POA2_off],
                            'POA2': [POA2_off],
                            'GHI': [GHI_off],
                            'ALB': [ALB_off]
                        }
                        
                        offset_df = pd.DataFrame(offset_data)
                        offset_df.to_csv(pyra_offset_file, index=False)
                        
                        historical_offset_df = offset_df
                        historical_offset_df.insert(loc=0, column='Date', value=datetime.now().strftime('%m/%d/%Y'))
                        historical_offset_df.to_csv(historical_offset_file, mode='a', index=False, header=False)
                        del historical_offset_df
                    
                    # if friday at 8pm send weekly report
                    if datetime.now().hour == 20 and datetime.now().minute == 00 and datetime.now().weekday() == 6:
                        send_email_process = multiprocessing.Process(target=send_email, args= (Inverter_data_tb,-7))
                        send_email_process.start()
                        
                        
                    #if new day    
                    if current_day != last_day:
                        ftp_process = multiprocessing.Process(target=FTP_worker, args=(Inverter_data_tb,)) #create a multiprossecing stream with FTP_worker function as a target
                        ftp_process.start() #start string
                        last_day = current_day #update last day 
                        print(f'new day: {current_day}')
                
    except KeyboardInterrupt:
        daq_Vout(DAQ_output_voltage=0)
        


if __name__ == "__main__":
    
    print('Program has started')
    main()

    

