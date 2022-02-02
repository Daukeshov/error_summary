import os
import io
import datetime

import pandas as pd
import numpy as np
import csv
from ftplib import FTP
import pyodbc
import re
import io
from io import StringIO
import string
from io import BytesIO

pd.options.mode.chained_assignment = None  # default='warn'
#####################################################################################################
date_now = str(datetime.datetime.now())
date_ftp = date_now[:-16]
date_u2000 = date_ftp.replace('-', '_')    #дата для CSV файла U2000
date_soem = date_ftp.replace('-', '')    #дата для CSV файла SOEM16
file = io.BytesIO()


# Подключение к FTP серверу SOEM и выгрузка данных из CSV файла в DataFrame
def ftp_soem():
    ftp = FTP('x.x.x.x')  # IP ftp
    ftp.login('xxxx', 'xxxx')
    ftp.cwd('INV')
    files_list = ftp.nlst()
    df_list = pd.DataFrame(files_list, columns=["A"])
    df_doc = df_list[df_list['A'].str.contains('soem16_NE_Inventory_' + date_soem)]
    soem = df_doc.iloc[-1, :]['A']
    ftp.retrbinary("RETR " + soem, open(soem, 'wb').write)
    global df_soem
    df_soem = pd.read_csv(soem)
    ftp.quit()

ftp_soem()


# Выгрузка списка всех сайтов из SOEM в DataFrame
def all_site_soem():
    global df_SOEM
    df_SOEM1 = df_soem[['NEAlias']]
    df_SOEM1.NEAlias = df_SOEM1.NEAlias.str.split('ML-').str[-1]
    df_SOEM1.NEAlias = df_SOEM1.NEAlias.str.split('CN-').str[-1]
    df_SOEM1['NEAlias'] = df_SOEM1['NEAlias'].str[:-2]
    df_SOEM2 = df_SOEM1.drop_duplicates(subset=['NEAlias'], keep='first')
    site_SPR = df_SOEM1[df_SOEM1['NEAlias'].str.contains('SPR')]
    site_SPR
    df_SOEM3 = pd.concat([df_SOEM2, site_SPR])
    df_SOEM4 = df_SOEM3.drop_duplicates(subset=['NEAlias'], keep=False)
    site_MSP = df_SOEM4[df_SOEM4['NEAlias'].str.contains('MSP')]
    site_MSP
    df_SOEM5 = pd.concat([df_SOEM4, site_MSP])
    df_SOEM6 = df_SOEM5.drop_duplicates(subset=['NEAlias'], keep=False)
    df_SOEM = df_SOEM6.reset_index(drop=True)

all_site_soem()


# Подключение к FTP серверу U2000 и выгрузка данных из CSV файла в DataFrame
def ftp_u200():
    ftp_1 = FTP('x.x.x.x')  # IP ftp
    ftp_1.login('xxxx', 'xxxx')
    ftp_1.cwd('ftproot/for_tr')  # path to dir
    files_list1 = ftp_1.nlst()
    df_list1 = pd.DataFrame(files_list1, columns=["B"])
    df_soem = df_list1[df_list1['B'].str.contains(date_u2000)]
    soem1 = df_soem.iloc[0, :]['B']
    ftp_1.retrbinary("RETR " + soem1, open(soem1, 'wb').write)
    global data
    data = pd.read_csv(soem1)
    ftp_1.quit()

ftp_u200()


# Выгрузка списка всех сайтов из U2000 в DataFrame
def all_site_u2000():
    global new_df
    new_df = data['BTS;BTS_IP;VLAN;BTS_MAC;VRRP_MAC'].str.split(';', expand=True)
    new_df.columns = ['NEAlias', 'BTS_IP', 'VLAN', 'BTS_MAC', 'VRRP_MAC']
    new_df = new_df.drop(new_df.columns[[1, 2, 3, 4]], axis=1)
    new_df['NEAlias'].replace(
        regex=[r"(.+)(?=[A-Z]{2}\d{4})", r"(?<=[A-Z]{2}\d{4})(.+)", r"(.+)(?=[A-Z]{3}\d{3})", r"(?<=[A-Z]{3}\d{3})(.+)"],
        value="", inplace=True)

all_site_u2000()


# Список всех Onair сайтов
def all_site_onair():
    df_result = pd.concat([df_SOEM, new_df], axis = 0)
    global df_all
    df_all = df_result.drop_duplicates(subset=['NEAlias'], keep='first')
    df_all = df_all.reset_index(drop=True)
    df_all.columns = ['NAME']
    df_all['Status'] = 'Onair'

all_site_onair()


# Подключение к серверу Atoll и выгрузка данных
def date_atoll():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=x.x.x.x', user='xxxx', password='xxxx')
    cursor = conn.cursor()
    query = "SELECT * FROM master.dbo.Sites"
    global df_atoll
    df_atoll = pd.read_sql(query, conn)

date_atoll()


# Вывод списка сайтов из Atoll с пустыми значениями TRtype, BSowner, Status и RegID
def empty_cell():
    global df_nan_cell
    none_cell1 = df_atoll.drop(df_atoll.columns[[1, 2, 3, 4, 5, 6, 10]], axis=1)
    none_cell1 = none_cell1.fillna(value=np.nan)
    none_cell = none_cell1.dropna()
    df_vse = pd.concat([none_cell1, none_cell])
    df_nan_cell = df_vse.drop_duplicates(subset=['NAME'], keep=False)
    df_nan_cell = df_nan_cell.reset_index(drop=True)

empty_cell()


# Сравнение Status сайтов и вывод списка сайтов с ошибками в DataFrame
def status_error():
    atoll1 = df_atoll.drop(df_atoll.columns[[1, 2, 3, 4, 5, 6, 7, 8, 10, 11]], axis=1)
    atoll2 = atoll1.fillna(value=np.nan)
    atoll3 = atoll2.dropna()
    df_merge = pd.concat([atoll3, df_all])
    df_finish1 = df_merge.drop_duplicates(subset=['NAME', 'Status'], keep=False)
    global osh
    osh = df_finish1[df_finish1.Status != 'Not exist']
    ID = osh['NAME'].str[:2]
    osh['Reg ID'] = ID
    osh = osh.reset_index(drop=True)

status_error()


writer = pd.ExcelWriter('Отчет по ошибкам Atoll.xlsx', engine='xlsxwriter')
df_nan_cell.to_excel(writer, 'Пустые ячейки')
osh.to_excel(writer, 'Status Eror')
writer.save()


