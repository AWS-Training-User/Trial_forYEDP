# -*- coding: utf-8 -*-
"""
Created on Sat May  9 15:37:45 2020
FileName - DataStimulation 
Usage -  To simulate DCS Data and Save to Database in every 2 minute intervals
Source Code: Chiranjib Kashyab
Author: 111280 (Moksith Bohra V)
Team : Madhavan Rangaswami, Subhramanyan E Edamana, Sugandh Kumar, Jithin Gopinathan Manakkulam, Parmita Ganguly, Allan Joseph Pothen, Moksith Bohra V
Module: Feedcycle Chemistry Point Solution

"""

'''
Process Workflow - 
1. Import required packages
2. Set Working Directory as defined in Common Inputs Excel
3. Connect to Database
4. Extract one random row from the Database Table
5. Get present timestamp and floor to nearest 5minute interval
6. Append the new timestamp to the randomly picked row
7. Append the new row to the end of the Database table
8. Close the connection to the database
'''



# Import Packages #
import pandas as pd
import pyodbc
import datetime as dt
import os



### Read Common Inputs from Excel File
commonInputs = pd.read_excel(r'D:\Feedcycle_Chemistry\Common_Inputs.xlsx') 
#Extract Working Directory details from commonInputs dataframe
workingDirectory = commonInputs[commonInputs['Name'] == 'Working_Directory']['Location'].values[0]
#Update working directory
os.chdir(workingDirectory)


# Connect to Database #
# Database Parameters #
server = commonInputs[commonInputs['Name'] == 'Server_Name']['Location'].values[0]
db = commonInputs[commonInputs['Name'] == 'Database_Name']['Location'].values[0]
UID = commonInputs[commonInputs['Name'] == 'User_ID']['Location'].values[0]
PWD = commonInputs[commonInputs['Name'] == 'Password']['Location'].values[0]
# Create the connection to Database
conn1 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + '; UID = ' + UID + '; PWD = ' + PWD + 'Trusted_Connection=yes')


# Reading Data from Table
df = pd.io.sql.read_sql_query("select * from [dbo].[DCS_Input_Data] WITH (NOLOCK)",conn1)


#Randomly sample one row for simulation
row = df.sample()




####data = pd.read_csv(r'.\Inputs\DCS_Pre_Simulation_Data.csv')  ###for testing

####row=data.loc[[0]]  ###for testing




# Get System Time in UTC Format.
# Grafana reads UTC time and reformats it to local time
now = dt.datetime.utcnow()


# Round the timestamp to nearest 5minute interval -- 10:56 becomes 10:56 and 10:51 becomes 10:50. Flooring action
rng = pd.date_range(now, periods=1, freq='2 min')
rng  = pd.Series(rng).dt.floor("2T")

#Repace old timestamp of sampled row with new timestamp
row['Timestamp'] = rng[0]







# Appending simulated row to Database
cursor = conn1.cursor()
for index,row in row.iterrows():
    cursor.execute("INSERT INTO [dbo].[DCS_Input_Data]([Timestamp],[Na_Reh_I],[Na_CPP_O],[Na_HRH_O],[Na_DMW_Pump],[Na_CEP_O],[CC_Con_Hot],[CC_Reh_I],[CC_Eco_I],[CC_CPP_O],[CC_DMW_Pump],[CC_CEP_O],[CC_HP_I],[SC_Eco_I],[SC_DMW_Pump],[Si_CPP_O],[Si_DMW_Pump],[pH_Eco_O],[DO_Eco_I],[DO_CEP_O],[CPP_Valve_Pos],[Na_HP_I],[DO_Con_Hot]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row['Timestamp'],row['Na_Reh_I'],row['Na_CPP_O'],row['Na_HRH_O'],row['Na_DMW_Pump'],row['Na_CEP_O'],row['CC_Con_Hot'],row['CC_Reh_I'],row['CC_Eco_I'],row['CC_CPP_O'],row['CC_DMW_Pump'],row['CC_CEP_O'],row['CC_HP_I'],row['SC_Eco_I'],row['SC_DMW_Pump'],row['Si_CPP_O'],row['Si_DMW_Pump'],row['pH_Eco_O'],row['DO_Eco_I'],row['DO_CEP_O'],row['CPP_Valve_Pos'],row['Na_HP_I'],row['DO_Con_Hot'])
    conn1.commit()
cursor.close()
conn1.close()
