#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, date

today=date.today()
yesterday = today- timedelta(days=1)
yesterday_month=yesterday.month
yesterday_year=yesterday.year
# print(yesterday_month,yesterday_year)

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/GK-DM/Crons/syed-reporting-83428b4082ce.json", scope)
client = gspread.authorize(creds)
# client


# # * Reading IPDs from 1.1 

# In[2]:


########################################### IPDs ( 1.1 Marketing IPD ATS sheet ) ##########################################

# Reading IPD ATS from 1.1 Marketing 
spreads = client.open_by_key("1oMjUi0rmavHn2QXrU9U5MlDQeuoiuOhuS8iwr0cN_Dg")
# Select the specific sheet by title
worksheet = spreads.worksheet('IPDs ATS')
# Specify the range you are interested in (for example, A1 to C10)
range_str = 'A25:M'
# Get the data in the specified range
data = worksheet.get(range_str)


# In[3]:


IPDs = pd.DataFrame(data[1:], columns=data[0])
IPDs = IPDs[IPDs['Apt ID'].astype(str).str.strip() != '']

# IPDs


# In[4]:


IPDs['Admission Date'] = pd.to_datetime(IPDs['Admission Date']).dt.date
IPDs=IPDs[IPDs['Admission Date']<today]
IPDs["Surgery Count"]=IPDs["Surgery Count"].astype(int)
# IPDs


# In[5]:


# Select the desired columns
IPDs_v1 = IPDs[["Admission Date","Sub-Category", "Final Disease", "Final_source", "City", "Final Category", "DIgital_Brand", "Surgery Count"]]

gp_IPDs = IPDs_v1.groupby(["Admission Date","Sub-Category", "Final Disease", "Final_source", "City", "Final Category",
                     "DIgital_Brand"])["Surgery Count"].sum().reset_index(name='Surgery Count')

gp_IPDs = gp_IPDs.sort_values(by='Surgery Count', ascending=False)

# gp_IPDs


# ### IPD 1 : to CPC CPL CAC movement - final

# In[6]:


#------------------------------------------ Reading IPD from CPC CPL CAC movement - Final--------------------------------------------

# Write the selected data to another Google Sheet- CPC CPL CAC Movement
spreads1 = client.open_by_key("1f9Emylgs4imtd6psaPbEtdCQXmgAQ12DX9Z4pVkYuAE") 

raw_data = pd.DataFrame(spreads1.worksheet('IPDs_Raw').get_all_records())
raw_data = raw_data[raw_data['Category'].astype(str).str.strip() != ''] 
# raw_data=pd.DataFrame(raw_data[1:], columns=raw_data[0])
raw_data.drop(["Don't Delete"], axis=1, inplace=True)
# raw_data


# In[7]:


raw_data['Month']=raw_data['Month'].replace({'Month': ""})


# In[8]:


check_month=yesterday.strftime("%b")+"'"+yesterday.strftime("%y")


# In[9]:


current_month_data =raw_data[(raw_data['Month']==check_month)]

if len(current_month_data)==0:
    raw_data['Actual Category']=raw_data['Actual Category'].replace({'Actual Category': ""})
    index = len(raw_data)
    index=index+2
    append=1
    
else:
    
    index=current_month_data.index
    index=index.min()
    index=index+1
    append=0
print("Current month ->",yesterday_month,": starting index ->",index,": append ->",append)


# In[10]:


IPD_CAC=gp_IPDs
IPD_CAC['Month']=check_month
IPD_CAC['Actual Category']=""
IPD_CAC=IPD_CAC[['Month','Actual Category','Sub-Category','Final Disease','Final_source','City','Final Category',
                 'DIgital_Brand','Surgery Count']]
# IPD_CAC


# In[11]:


import csv

csvFile = 'IPD_in_CPC_CPL_CAC_movement.csv'
IPD_CAC.to_csv(csvFile,index=False)

# spreadsheetId = spreads1
# sh = client.open_by_key(spreadsheetId)

sheetName = f"IPDs_Raw!A{index}:I"

spreads1.values_clear(sheetName)  
print(f'IPD is cleared from {sheetName} in CPC CPL CAC Movement ')
spreads1.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile)))})
print(f'IPD is written to {sheetName} in CPC CPL CAC Movement ')

spreads1.worksheet('IPDs_Raw').add_rows(1) # adding extra row in sheet
print(f"One blank Row Appended in {sheetName}")

# ### IPD 2 : to WOW CPL CAC Movement

# In[12]:


#------------------------------------------ Reading IPD from WoW - CPL CAC Movement----------------------------------------

spreads2 = client.open_by_key("1fl2Mi1Ei27E-D4RprchF7DZSR0g4R0zVfEbdl-9v8qM")

raw_data = pd.DataFrame(spreads2.worksheet('IPDs').get_all_records())
# raw_data.head()


# In[13]:


raw_data['Admission Date']=raw_data['Admission Date'].replace({'Admission Date': ''})

raw_data['Admission Date']=pd.to_datetime(raw_data['Admission Date'])
raw_data['F_Month']=raw_data['Admission Date'].dt.month
raw_data['F_Year']=raw_data['Admission Date'].dt.year
raw_data = raw_data[raw_data['Sub-Category...13'].astype(str).str.strip() != '']


# In[14]:


current_month_data =raw_data[(raw_data['F_Month']==yesterday_month)&(raw_data['F_Year']==yesterday_year)]
if len(current_month_data)==0:
    
    index = len(raw_data)
    index=index+2
    append=1
    
    
else:
    
    index=current_month_data.index
    index=index.min()
    index=index+1
    append=0
print("Current month ->",yesterday_month,": starting index ->",index,": append ->",append)


# In[15]:


IPDs_v2 = IPDs[['Admission Date','Final Category','City','Final Disease','Final_source','Surgery Count','DIgital_Brand']]


gp_IPDs_v2 = IPDs_v2.groupby(['Admission Date','Final Category','City','Final Disease','Final_source',
                           'DIgital_Brand'])["Surgery Count"].sum().reset_index(name='Surgery_Count')
# gp_IPDs_v2


# In[16]:


gp_IPDs_v2 =gp_IPDs_v2.sort_values(by=['Admission Date'], ascending=[True])
gp_IPDs_v2 = gp_IPDs_v2[['Admission Date','Final Category','City','Final Disease','Final_source','Surgery_Count','DIgital_Brand']]
# gp_IPDs_v2


# In[17]:


#------------------------------------------ Writing IPD in CPC CPL CAC Movement--------------------------------------------

csvFile = 'IPD_in_WoW_CPL CAC Movement.csv'
gp_IPDs_v2.to_csv(csvFile,index=False)

sheetName = f"IPDs!A{index}:G"

spreads2.values_clear(sheetName)  
print(f'IPD is cleared from {sheetName} in WoW - CPL CAC Movement ')
spreads2.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile)))})
print(f'IPD is written to {sheetName} in WoW - CPL CAC Movement ')

spreads2.worksheet('IPDs').add_rows(1) # adding extra row in sheet
print(f"One blank Row Appended in {sheetName}")

# # -----------------------------------------------------------------------------------------------------------

# # * Reading Spends from Spends for CPL CAC

# In[18]:


########################################### Reading Spends from Spends for CPL CAC  ##########################################

spreads = client.open_by_key("157lg802h4257XYnqo4P0XfsYaHh_1ZwM57B2E6NZXJ4")
# Select the specific sheet by title
worksheet = spreads.worksheet('Consolidate')
# Specify the range you are interested in (for example, A1 to C10)
range_str = 'A:N'
# Get the data in the specified range
data = worksheet.get(range_str)


# In[19]:


spends = pd.DataFrame(data[1:], columns=data[0])
spends = spends[spends['Month number'].astype(str).str.strip() != '']

# spends.head()


# In[20]:


spends=spends[['Month number','Amount spent','Impressions','Clicks','Conversions','Required Category',
               'Sub Category','Disease','Location','Source','Brand Confirmation']]

spends["Month number"]=spends["Month number"].astype(int)
spends=spends[spends['Month number']==yesterday_month]

# spends


# ### Spends 1 :  to CPC CPL CAC movement - Final

# In[21]:


#------------------------------------ Reading Spends from CPC CPL CAC movement - Final-------------------------------------

spreads3 = client.open_by_key("1f9Emylgs4imtd6psaPbEtdCQXmgAQ12DX9Z4pVkYuAE")
worksheet = spreads3.worksheet('Spends_Raw')
range_str = 'A:N'
data = worksheet.get(range_str)

raw_data = pd.DataFrame(data[1:], columns=data[0])
raw_data = raw_data[raw_data['Month'].astype(str).str.strip() != '']

# raw_data.head(3)


# In[22]:


check_month=yesterday.strftime("%b")+"'"+yesterday.strftime("%y")


# In[23]:


current_month_data =raw_data[(raw_data['Month']==check_month)]
if len(current_month_data)==0:
    
    index = len(raw_data)
    index=index+2
    append=1
    
    
else:
    
    index=current_month_data.index
    index=index.min()
    index=index+1
    append=0
print("Current month ->",yesterday_month,": starting index ->",index,": append ->",append)


# In[24]:


spends_new=spends[['Month number','Amount spent','Impressions','Clicks','Conversions','Required Category','Sub Category',
                   'Disease','Location','Source','Brand Confirmation']]
spends_new['Month number']=check_month

# spends_new


# In[25]:


spends_new['Amount spent']=spends_new['Amount spent'].astype(float)
spends_new['Impressions']=spends_new['Impressions'].astype(float)
spends_new['Clicks']=spends_new['Clicks'].astype(float)
spends_new['Conversions']=spends_new['Conversions'].astype(float)

gp_spends = spends_new.groupby(['Month number','Required Category','Sub Category','Disease','Location','Source',
                                'Brand Confirmation'])[['Amount spent','Impressions','Clicks','Conversions']].sum().reset_index()
#ordering columns
gp_spends=gp_spends[['Month number','Amount spent','Impressions','Clicks','Conversions','Required Category',
                     'Sub Category','Disease','Location','Source','Brand Confirmation']]

# gp_spends


# In[26]:


# ---------- Writing ---------- #
import csv
csvFile = 'Spends_in_CPC_CPL_CAC_Movement.csv'
gp_spends.to_csv(csvFile,index=False)

sheetName = f"Spends_Raw!A{index}:K"

spreads3.values_clear(sheetName)  
print(f'Spends is cleared from {sheetName} in CPC_CPL_CAC_Movement ')
spreads3.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile)))})
print(f'Spends is written to {sheetName} in CPC_CPL_CAC_Movement ')

spreads3.worksheet('Spends_Raw').add_rows(1) # adding extra row in sheet
print(f"One blank Row Appended in {sheetName}")

# ### Spends 2 : Reading from Marketing spends 2023 & Writing to WOW CPL CAC Movement

# In[27]:


########################################### Reading Spends ( Spends from Marketing spends 2023) ##########################################

spreads = client.open_by_key("1GrdFV2d6CWFPvB-rmSZBzYRfKTAGZlWFo9U4nFTbpy8")
# Select the specific sheet by title
worksheet = spreads.worksheet('Consolidated Spends')
# Specify the range you are interested in (for example, A1 to C10)
range_str = 'A:O'
# Get the data in the specified range
data = worksheet.get(range_str)

spends_wow = pd.DataFrame(data[1:], columns=data[0])
spends_wow = spends_wow[spends_wow['Campaign name'].astype(str).str.strip() != '']

# spends_wow.head()


# In[28]:


spends_wow=spends_wow[['Date','Required Category','Disease','City','Source','Brand Confirmation','Amount spent',
                       'Impressions','Clicks','Conversions']]
# spends_wow


# In[29]:


spends_wow['Amount spent']=spends_wow['Amount spent'].astype(float)
spends_wow['Impressions']=spends_wow['Impressions'].astype(float)
spends_wow['Clicks']=spends_wow['Clicks'].astype(float)
spends_wow['Conversions']=spends_wow['Conversions'].astype(float)

gp_spends_wow = spends_wow.groupby(['Date','Required Category','Disease','City','Source',
                                    'Brand Confirmation'])[['Amount spent','Impressions','Clicks','Conversions']].sum().reset_index()
#ordering columns
gp_spends_wow=gp_spends_wow[['Date','Required Category','Disease','City','Source','Brand Confirmation','Amount spent',
                       'Impressions','Clicks','Conversions']]

# gp_spends_wow


# In[30]:


spreads4 = client.open_by_key("1fl2Mi1Ei27E-D4RprchF7DZSR0g4R0zVfEbdl-9v8qM")
worksheet = spreads4.worksheet('Spends')
range_str = 'A:J'
data = worksheet.get(range_str)

raw_data = pd.DataFrame(data[1:], columns=data[0])
raw_data = raw_data[raw_data['Date'].astype(str).str.strip() != '']

# raw_data.head(3)


# In[31]:


# check_month=yesterday.strftime("%b")+"'"+yesterday.strftime("%y")
raw_data['Date']=raw_data['Date'].replace({'Date': ''})

raw_data['Date']=pd.to_datetime(raw_data['Date'])
raw_data['Month']=raw_data['Date'].dt.month


# In[32]:


current_month_data =raw_data[(raw_data['Month']==yesterday_month)]
if len(current_month_data)==0:
    
    index = len(raw_data)
    index=index+2
    append=1
    
    
else:
    
    index=current_month_data.index
    index=index.min()
    index=index+1
    append=0
print("Current month ->",yesterday_month,": starting index ->",index,": append ->",append)


# In[33]:


# ---------- Writing ---------- #
import csv
csvFile = 'Spends_in_WoW - CPL CAC Movement.csv'
gp_spends_wow.to_csv(csvFile,index=False)

# spreadsheetId = '1fl2Mi1Ei27E-D4RprchF7DZSR0g4R0zVfEbdl-9v8qM'
# sh = client.open_by_key(spreadsheetId)

sheetName = f"Spends!A{index}:K"

spreads4.values_clear(sheetName)  
print(f'Spends is cleared from {sheetName} in WoW - CPL CAC Movement ')
spreads4.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile)))})
print(f'Spends is written to {sheetName} in WoW - CPL CAC Movement ')

spreads4.worksheet('Spends').add_rows(1) # adding extra row in sheet
print(f"One blank Row Appended in {sheetName}")

# # -----------------------------------------------------------------------------------------------------------

# # * Reading OPDs from Growth 1.4

# In[34]:


########################################### Reading OPDs from 1.4 ##########################################

spreads = client.open_by_key("1DF9G7Td_v-fxi1_P87InccJeYFGD7Jk243CsGfr_dEU")
# Select the specific sheet by title
worksheet = spreads.worksheet('Appts')
# Specify the range you are interested in (for example, A1 to C10)
range_str = 'A:AZ'
# Get the data in the specified range
data = worksheet.get(range_str)

OPD = pd.DataFrame(data[1:], columns=data[0])
OPD = OPD[OPD['Appt_id'].astype(str).str.strip() != '']

OPD['Year']=OPD['Year'].astype(int)
OPD['ApptMonth']=OPD['ApptMonth'].astype(int)

OPD=OPD[(OPD['Year']==yesterday_year)&(OPD['ApptMonth']==yesterday_month)]

OPD['OPD_Booked_Flag']=OPD['OPD_Booked_Flag'].astype(int)
OPD['OPD_Flag']=OPD['OPD_Flag'].astype(int)

# OPD


# ### OPDs 1 : to CPC CPL CAC

# In[35]:


OPD_CAC_CPL=OPD[['ApptMonth','Current_Category_Leads','Current_Team_leads','Final Category','SurgeryType','Source',
                'Source Type','CityFinal','OPD_Booked_Flag','OPD_Flag']]

OPD_CAC_CPL=OPD_CAC_CPL.groupby(['ApptMonth','Current_Category_Leads','Current_Team_leads','Final Category','SurgeryType','Source',
                'Source Type','CityFinal'])[['OPD_Booked_Flag','OPD_Flag']].sum().reset_index()

# OPD_CAC_CPL


# In[36]:


#------------------------------------ Reading OPDs from CPC CPL CAC movement - Final-------------------------------------

spreads3 = client.open_by_key("1f9Emylgs4imtd6psaPbEtdCQXmgAQ12DX9Z4pVkYuAE")
worksheet = spreads3.worksheet('OPDs_Raw')
range_str = 'A:J'
data = worksheet.get(range_str)

raw_data = pd.DataFrame(data[1:], columns=data[0])

raw_data = raw_data[raw_data['ApptMonth'].astype(str).str.strip() != '']
raw_data['ApptMonth']=raw_data['ApptMonth'].replace({'ApptMonth': ''})

# raw_data.head(3)


# In[37]:


check_month=yesterday.strftime("%b")+"'"+yesterday.strftime("%y")


# In[38]:


current_month_data =raw_data[(raw_data['ApptMonth']==check_month)]
if len(current_month_data)==0:
    
    index = len(raw_data)
    index=index+2
    append=1
    
    
else:
    
    index=current_month_data.index
    index=index.min()
    index=index+1
    append=0
print("Current month ->",yesterday_month,": starting index ->",index,": append ->",append)


# In[39]:


OPD_CAC_CPL['ApptMonth']=check_month
# OPD_CAC_CPL


# In[40]:


# ---------- Writing ---------- #
import csv
csvFile = 'OPDs_in_CPC CPL CAC.csv'
OPD_CAC_CPL.to_csv(csvFile,index=False)


sheetName = f"OPDs_Raw!A{index}:K"

spreads3.values_clear(sheetName)  
print(f'OPDs is cleared from {sheetName} in CPC CPL CAC')
spreads3.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile)))})
print(f'OPDs is written to {sheetName} in CPC CPL CAC')

spreads3.worksheet('OPDs_Raw').add_rows(1) # adding extra row in sheet
print(f"One blank Row Appended in {sheetName}")

# # -----------------------------------------------------------------------------------------------------------

# # * Reading Leads from Daily CAC

# In[41]:


########################################### Reading Leads ( Leads form daily CAC ) ##########################################

spreads = client.open_by_key("19ZrIhgtQTqQoE2yzEH2V8NBarEYGqh7T78oWsr59olw")
# Select the specific sheet by title
worksheet = spreads.worksheet('Leads')
# Specify the range you are interested in (for example, A1 to C10)
range_str = 'A:U'
# Get the data in the specified range
data = worksheet.get(range_str)

CAC_leads = pd.DataFrame(data[1:], columns=data[0])
CAC_leads = CAC_leads[CAC_leads['Mobile'].astype(str).str.strip() != '']
CAC_leads=CAC_leads[(CAC_leads['Cateee'] != 'PL')]
# CAC_leads.head()


# ### Leads 1 : Writing Leads to CPC CPL CAC movement - Final

# In[42]:


leads=CAC_leads[['Enq_Month','Cateee','Required Category','Last Final City_Standardized','Final Disease Last',
                     'Source','Brand Confirmation']]
# leads


# In[43]:


gp_leads = leads.groupby(['Enq_Month','Cateee','Required Category','Last Final City_Standardized','Final Disease Last',
                              'Source','Brand Confirmation']).size().reset_index(name="Leads count")

gp_leads =gp_leads.sort_values(by=['Leads count'], ascending=[False])


gp_leads['Enq_Month']=gp_leads['Enq_Month'].astype(int)
gp_leads=gp_leads[gp_leads['Enq_Month']==yesterday_month]


gp_leads['Brand Confirmation'] = np.where(gp_leads['Brand Confirmation'] == "Non Brand", "Digital",
                                             gp_leads['Brand Confirmation'])

# gp_leads


# In[44]:


#------------------------------------ Reading Spends from CPC CPL CAC movement - Final-------------------------------------

spreads4 = client.open_by_key("1f9Emylgs4imtd6psaPbEtdCQXmgAQ12DX9Z4pVkYuAE")
worksheet = spreads4.worksheet('Leads_Raw')
range_str = 'A:N'
data = worksheet.get(range_str)

raw_data = pd.DataFrame(data[1:], columns=data[0])
raw_data = raw_data[raw_data['Month'].astype(str).str.strip() != '']

# raw_data.head(3)


# In[45]:


check_month=yesterday.strftime("%b")+"'"+yesterday.strftime("%y")


# In[46]:


current_month_data =raw_data[(raw_data['Month']==check_month)]
if len(current_month_data)==0:
    
    index = len(raw_data)
    index=index+2
    append=1
    
    
else:
    
    index=current_month_data.index
    index=index.min()
    index=index+1
    append=0
print("Current month ->",yesterday_month,": starting index ->",index,": append ->",append)


# In[47]:


# ---------- Writing ---------- #
# import csv
gp_leads['Enq_Month']=check_month
csvFile = 'Leads_in_CPC_CPL_CAC_Movement.csv'
gp_leads.to_csv(csvFile,index=False)

sheetName = f"Leads_Raw!A{index}:H"

spreads4.values_clear(sheetName)  
print(f'Leads is cleared from {sheetName} in CPC_CPL_CAC_Movement ')
spreads4.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile)))})
print(f'Leads is written to {sheetName} in CPC_CPL_CAC_Movement ')

spreads4.worksheet('Leads_Raw').add_rows(1) # adding extra row in sheet
print(f"One blank Row Appended in {sheetName}")

# ### Leads 2 : Writing Leads to WOW from 7day sheet

# In[53]:


week_on_week = CAC_leads

# week_on_week['Enq_Month'].astype(int)
week_on_week['Enq_MM'] = pd.to_datetime(week_on_week['Enq_Date']).dt.month
week_on_week = week_on_week[(week_on_week['Enq_MM'] == yesterday_month)]

week_on_week = week_on_week[['Enq_Date','Category_Current_STD','Cateee','Last Final City_Standardized',
                             'Final Disease Last','Source','Brand Confirmation']]

week_on_week = week_on_week.groupby(['Enq_Date', 'Category_Current_STD', 'Cateee',
                                              'Last Final City_Standardized','Final Disease Last', 'Source',
                                              'Brand Confirmation']).size().reset_index(name='count')

week_on_week.reset_index(drop=True, inplace=True)
# week_on_week


# In[54]:


# reading existing data
sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1fl2Mi1Ei27E-D4RprchF7DZSR0g4R0zVfEbdl-9v8qM/edit#gid=552863216')

raw_data = pd.DataFrame(sheet.worksheet('Leads').get_all_records())

raw_data = raw_data[raw_data['Enq_Date'].astype(str).str.strip() != '']

raw_data['Enq_Date']=raw_data['Enq_Date'].replace({'Enq_Date': ''})
raw_data['Enq_Date']=pd.to_datetime(raw_data['Enq_Date'])
raw_data['Month']=raw_data['Enq_Date'].dt.month


# In[55]:


current_month_data =raw_data[(raw_data['Month']==yesterday_month)]
if len(current_month_data)==0:
    
    index = len(raw_data)
    index=index+2
    append=1
    
    
else:
    
    index=current_month_data.index
    index=index.min()
    index=index+1
    append=0
print("Current month ->",yesterday_month,": starting index ->",index,": append ->",append)


# In[56]:


# import csv

week_on_week = week_on_week.sort_values(by='Enq_Date', ascending=True)

csvFile = 'wow_leads.csv'
week_on_week.to_csv(csvFile,index=False)

spreadsheetId = '1fl2Mi1Ei27E-D4RprchF7DZSR0g4R0zVfEbdl-9v8qM'
sh = client.open_by_key(spreadsheetId)

sheetName = f"Leads!A{index}:H"
sheet.values_clear(sheetName)  
print(f'Leads is cleared from {sheetName} in WOW CPL CAC Movement ')

sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile)))})
print(f'Data is written to {sheetName} in WOW CPL CAC Movement')

sh.worksheet('Leads').add_rows(1) # adding extra row in sheet
print(f"One blank Row Appended in {sheetName}")

###################################################### Email ########################################
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from datetime import date, timedelta

today=date.today()
yesterday = today- timedelta(days=1)

subject = "[Cron runed] WoW - CPL CAC Movement & CPC CPL CAC movement - Final Updated till ["+str(yesterday)+"]"

body = "Hi,\n \n WoW - CPL CAC Movement & CPC CPL CAC movement - Final Updated till "+str(yesterday)+"\nBelow are the sheet Links:  \n 1.WoW - CPL CAC Movement - https://docs.google.com/spreadsheets/d/1fl2Mi1Ei27E-D4RprchF7DZSR0g4R0zVfEbdl-9v8qM/edit?usp=sharing \n 2. CPC CPL CAC movement - Final - https://docs.google.com/spreadsheets/d/1f9Emylgs4imtd6psaPbEtdCQXmgAQ12DX9Z4pVkYuAE/edit?usp=sharing "

sender_email = ["marketing.reports@pristyncare.com"]
receiver_email = ["gaurav.kumar2@pristyncare.com","atiullah.ansari@pristyncare.com","deepinder.singh@pristyncare.com","yogesh@pristyncare.com","saurabh.garg@pristyncare.com"]
# cc_email = ["gagan.arora@pristyncare.com","himanshu.jindal@pristyncare.com","gaurav.kumar2@pristyncare.com"]

password = "Target@7000" #m

email = MIMEMultipart()
email["From"] = ', '.join(sender_email)
email["To"] =  ', '.join(receiver_email)
# email["CC"] = ', '.join(cc_email)
email["Subject"] = subject


email.attach(MIMEText(body,"plain"))
bodytext=""

session = smtplib.SMTP('smtp.gmail.com', 587) 
session.starttls() 
session.login(sender_email[0],password) 
text = email.as_string()
session.sendmail(sender_email, receiver_email,text)
#                      +cc_email,text)
print("mail sent")