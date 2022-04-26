import celery
from celery import shared_task
from celery import current_task

from django.shortcuts import render
import json
from pandas import json_normalize
from django.core.files.storage import FileSystemStorage
import pandas as pd
import numpy as np
import os
import warnings
import math
from celery_progress.backend import ProgressRecorder

warnings.filterwarnings('ignore')

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent





@shared_task
def BAJAJ_PL_MIS_TASK(A1,B1):
    # print(A1)
    # print(B1)
    A2 = json.loads(A1)
    # print(A2)
    A = pd.DataFrame.from_dict(A2)
    # print(A)
    A.head()
    B2 = json.loads(B1)
    B = pd.DataFrame.from_dict(B2)
    B.head()

    A.to_excel(r'media/BAJAJ-PL/MIS/MAR 22/BAJAJ-PL ALLOCATION.xlsx', index=False)
    B.to_excel(r'media/BAJAJ-PL/MIS/MAR 22/BAJAJ-PL PAID FILE.xlsx', index=False)

    B1 = pd.DataFrame(B.groupby('AGREEMENTID')['PAID AMOUNT'].sum()).reset_index()
    B2 = pd.DataFrame(B.groupby(['AGREEMENTID', 'TC'])['PAID AMOUNT'].count()).reset_index()

    ECS = pd.DataFrame(B.groupby(['AGREEMENTID', 'MODE'])['PAID AMOUNT'].sum()).reset_index()

    print(current_task)

    progress_recorder=ProgressRecorder(task= current_task)

    progress_recorder.set_progress(current = 1, total = 7)

    ECS = ECS[ECS['MODE'] == 'ECS']

    ECS.rename({'PAID AMOUNT': 'ECS'}, axis=1, inplace=True)

    B1.head()

    B1 = B1.merge(ECS, how='left')

    B1.drop('MODE', axis=1, inplace=True)

    B1.fillna(0, inplace=True)

    B1['FINAL PAID AMOUNT'] = B1['PAID AMOUNT'] - B1['ECS']

    B1.head()

    B2.drop('PAID AMOUNT', axis=1, inplace=True)

    B2.rename({'TC': 'PAID TC', 'AGREEMENTID': 'LOAN_NUMBER'}, axis=1, inplace=True)

    B2

    B1.head()

    B1.drop(['PAID AMOUNT', 'ECS'], axis=1, inplace=True)

    B1.rename({'FINAL PAID AMOUNT': 'PAID AMOUNT'}, axis=1, inplace=True)

    B1.head()

    B1.rename({'AGREEMENTID': 'LOAN_NUMBER'}, axis=1, inplace=True)

    A.head(1)

    B2.head(1)

    B1

    progress_recorder.set_progress(current= 2, total = 7)

    A = A.merge(B2, left_on='LOAN AGREEMENT NO', right_on='LOAN_NUMBER', how='left')

    A = A.merge(B1, left_on='LOAN AGREEMENT NO', right_on='LOAN_NUMBER', how='left')

    A.head()

    A.drop(['LOAN_NUMBER_x', 'LOAN_NUMBER_y'], axis=1, inplace=True)

    A.head(1)

    for i in range(0, len(A['LOAN AGREEMENT NO'])):
        if str(A.loc[i, 'PAID AMOUNT']) == 'nan':
            A.loc[i, 'MOHAK STATUS'] = 'UNPAID'
        elif str(A.loc[i, 'PAID AMOUNT']) != 'nan':
            A.loc[i, 'MOHAK STATUS'] = 'PAID'

    progress_recorder.set_progress(current= 3, total = 7)

    A.head(1)

    for i in range(0, len(A['LOAN AGREEMENT NO'])):
        if str(A.loc[i, 'PAID TC']) == 'nan':
            A.loc[i, 'PAID TC'] = '--'
    for i in range(0, len(A['LOAN AGREEMENT NO'])):
        if str(A.loc[i, 'PAID AMOUNT']) == 'nan':
            A.loc[i, 'PAID AMOUNT'] = 0

    A.head(1)

    A['PAID TC'].unique()

    A.rename({'MOHAK STATUS': 'STATUS'}, axis=1, inplace=True)

    progress_recorder.set_progress(current= 4, total = 7)

    for i in range(0, len(A['LOAN AGREEMENT NO'])):
        A.loc[i, 'OD AMOUNT'] = (A.loc[i, 'BOM_BUCKET'] + 1) * A.loc[i, 'INST AMT']

    A.head(1)

    A['STATUS'].unique()

    for i in range(0, len(A['PAID AMOUNT'])):
        if A.loc[i, 'STATUS'] == 'PAID':
            if A.loc[i, 'PAID AMOUNT'] <= A.loc[i, 'OD AMOUNT']:
                A.loc[i, 'PAID FEEDBACK'] = 'LESS THAN DEMAND'
            else:
                A.loc[i, 'PAID FEEDBACK'] = 'MORE THAN DEMAND'
        else:
            A.loc[i, 'PAID FEEDBACK'] = 'UNPAID'

    A.to_excel(r'media/BAJAJ-PL/MIS/MAR 22/MASTER_FILE_BAJAJ-PL.xlsx', index=False)

    progress_recorder.set_progress(current= 5, total = 7)

    for i in range(0, len(A['LOAN AGREEMENT NO'])):
        if A.loc[i, 'STATUS'] == 'PAID':
            if A.loc[i, 'PAID TC'] != 'SYS. PAID':
                if A.loc[i, 'TC NAME'] != A.loc[i, 'PAID TC']:
                    print("ACTUAL TC-", A.loc[i, 'TC NAME'], "PAID TC-", A.loc[i, 'PAID TC'])

    P = pd.DataFrame(A.groupby(['PAID FEEDBACK', 'PAID TC'])['PAID AMOUNT'].sum().reset_index())

    P.head()

    a = []
    for i in range(0, len(P['PAID AMOUNT'])):
        if P.loc[i, 'PAID FEEDBACK'] == 'UNPAID':
            a.append(i)

    P.drop(a, axis=0, inplace=True)

    P

    for i in range(0, len(P['PAID TC'])):
        if P.loc[i, 'PAID FEEDBACK'] == 'LESS THAN DEMAND':
            if (P.loc[i, 'PAID AMOUNT'] >= 500000):
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '5%'
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 400000) and (P.loc[i, 'PAID AMOUNT'] < 500000):
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '4%'
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 300000) and (P.loc[i, 'PAID AMOUNT'] < 400000):
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '3%'
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 200000) and (P.loc[i, 'PAID AMOUNT'] < 300000):
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '2%'
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 100000) and (P.loc[i, 'PAID AMOUNT'] < 200000):
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '1%'
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '0%'
            else:
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '0%'
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '0%'
        elif P.loc[i, 'PAID FEEDBACK'] == 'MORE THAN DEMAND':
            if (P.loc[i, 'PAID AMOUNT'] >= 650000):
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '5%'
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 550000) and (P.loc[i, 'PAID AMOUNT'] < 650000):
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '4%'
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 450000) and (P.loc[i, 'PAID AMOUNT'] < 550000):
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '3%'
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 350000) and (P.loc[i, 'PAID AMOUNT'] < 450000):
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '2%'
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '0%'
            elif (P.loc[i, 'PAID AMOUNT'] >= 250000) and (P.loc[i, 'PAID AMOUNT'] < 350000):
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '1%'
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '0%'
            else:
                P.loc[i, 'MORE THAN DEMAND PAYOUT%'] = '0%'
                P.loc[i, 'LESS THAN DEMAND PAYOUT%'] = '0%'

    P

    P.to_excel(r'media/BAJAJ-PL/MIS/MAR 22/BAJAJ-PL TC MIS.xlsx', index=False)

    progress_recorder.set_progress(current= 6, total = 7)

    A.head()

    SS = pd.DataFrame(A.groupby(['BOM_BUCKET'])['POS', 'PAID AMOUNT'].sum().reset_index())

    SS.head()

    for i in range(0, len(SS['BOM_BUCKET'])):
        SS.loc[i, 'PERFORMANCE'] = (SS.loc[i, 'PAID AMOUNT'] / SS.loc[i, 'POS']) * 100

    for i in range(0,len(SS['PERFORMANCE'])):
        SS.loc[i,'PERFORMANCE']=round(SS.loc[i,'PERFORMANCE'], 2)
        SS.loc[i, 'PAID AMOUNT'] = round(SS.loc[i, 'PAID AMOUNT'], 2)
        SS.loc[i, 'POS'] = round(SS.loc[i, 'POS'], 2)

    SS.head()

    SS.to_excel(r'media/BAJAJ-PL/MIS/MAR 22/BAJAJ-PL MIS.xlsx', index=False)

    A.head(1)

    for i in range(0, len(A['OD AMOUNT'])):
        if A.loc[i, 'BOM_BUCKET'] == 3:
            if (A.loc[i, 'PAID AMOUNT'] * 15) / 100 > (A.loc[i, 'OD AMOUNT'] * 15) / 100:
                A.loc[i, 'BILLING%'] = '15%'
                A.loc[i, 'PAYOUT'] = (A.loc[i, 'OD AMOUNT'] * 15) / 100
            else:
                A.loc[i, 'BILLING%'] = '15%'
                A.loc[i, 'PAYOUT'] = (A.loc[i, 'PAID AMOUNT'] * 15) / 100
        elif A.loc[i, 'BOM_BUCKET'] == 4:
            if (A.loc[i, 'PAID AMOUNT'] * 16) / 100 > (A.loc[i, 'OD AMOUNT'] * 16) / 100:
                A.loc[i, 'BILLING%'] = '16%'
                A.loc[i, 'PAYOUT'] = (A.loc[i, 'OD AMOUNT'] * 16) / 100
            else:
                A.loc[i, 'BILLING%'] = '16%'
                A.loc[i, 'PAYOUT'] = (A.loc[i, 'PAID AMOUNT'] * 16) / 100
        elif A.loc[i, 'BOM_BUCKET'] == 5:
            if (A.loc[i, 'PAID AMOUNT'] * 17) / 100 > (A.loc[i, 'OD AMOUNT'] * 17) / 100:
                A.loc[i, 'BILLING%'] = '17%'
                A.loc[i, 'PAYOUT'] = (A.loc[i, 'OD AMOUNT'] * 17) / 100
            else:
                A.loc[i, 'BILLING%'] = '17%'
                A.loc[i, 'PAYOUT'] = (A.loc[i, 'PAID AMOUNT'] * 17) / 100

    A.to_excel(r'media/BAJAJ-PL/Billing/MAR 22/BAJAJ-PL Billing.xlsx', index=False)

    A23 = pd.DataFrame(A.groupby(['BOM_BUCKET', 'BILLING%'])['PAYOUT'].sum()).reset_index()

    A23['PAYOUT'] = round(A23['PAYOUT'], 2)

    A23.to_excel(r'media/BAJAJ-PL/Billing/MAR 22/BAJAJ-PL Billing MIS.xlsx', index=False)

    progress_recorder.set_progress(current= 7, total = 7)


@shared_task
def IDFC_TW_MIS_TASK(A1,B1):
    A2 = json.loads(A1)
    A = pd.DataFrame.from_dict(A2)
    A.head()
    print(A.shape)
    B2 = json.loads(B1)
    B = pd.DataFrame.from_dict(B2)
    B.head()
    print(B.shape)
    A=A.reset_index(drop=True)
    B=B.reset_index(drop=True)

    progress_recorder = ProgressRecorder(task=current_task)

    progress_recorder.set_progress(current=1, total=7)

    A.to_excel(r'media/IDFC_TW/MIS/MAR 22/IDFC-TW ALLOCATION.xlsx', index=False)
    B.to_excel(r'media/IDFC_TW/MIS/MAR 22/IDFC-TW PAID FILE.xlsx', index=False)

    B1 = pd.DataFrame(B.groupby('AGREEMENTID')['PAID AMOUNT'].sum()).reset_index()

    print(B1)

    for i in range(0, len(A['AGREEMENTID'])):
        for k in range(0, len(B['AGREEMENTID'])):
            if (A.loc[i, 'AGREEMENTID'] == B.loc[k, 'AGREEMENTID']) and (B.loc[k, 'AGAINST'] != 'FORECLOSE') and (B.loc[k, 'AGAINST'] != 'SETTLEMENT'):
                for j in range(0, len(B1['AGREEMENTID'])):
                    if A.loc[i, 'AGREEMENTID'] == B1.loc[j, 'AGREEMENTID']:
                        if A.loc[i, 'BKT'] == 0:
                            if B.loc[k, 'AGAINST'] == 'RT':
                                A.loc[i, 'STATUS'] == 'RT'
                            elif B1.loc[j, 'PAID AMOUNT'] >= A.loc[i, 'EMI']:
                                A.loc[i, 'STATUS'] = 'SB'
                            elif B1.loc[j, 'PAID AMOUNT'] < A.loc[i, 'EMI']:
                                A.loc[i, 'STATUS'] = 'PART PAID'
                            else:
                                A.loc[i, 'STATUS'] = 'UNPAID'
                        elif A.loc[i, 'BKT'] != 1:
                            a = (A.loc[i, 'BKT'] + 1) * A.loc[i, 'EMI']
                            b = A.loc[i, 'EMI'] + A.loc[i, 'EMI']
                            if B.loc[k, 'AGAINST'] == 'RT':
                                A.loc[i, 'STATUS'] = 'RT'
                            elif (B1.loc[j, 'PAID AMOUNT'] >= a) or (B1.loc[j, 'PAID AMOUNT'] >= A.loc[i, 'POS']):
                                A.loc[i, 'STATUS'] = 'NM'
                            elif (B1.loc[j, 'PAID AMOUNT'] >= b) and ((B1.loc[j, 'PAID AMOUNT'] < a) and (
                                    B1.loc[j, 'PAID AMOUNT'] < A.loc[i, 'POS'])):
                                A.loc[i, 'STATUS'] = 'RB'
                            elif (B1.loc[j, 'PAID AMOUNT'] >= A.loc[i, 'EMI']) and (B1.loc[j, 'PAID AMOUNT'] < b):
                                A.loc[i, 'STATUS'] = 'SB'
                            elif B1.loc[j, 'PAID AMOUNT'] < A.loc[i, 'EMI']:
                                A.loc[i, 'STATUS'] = 'PART PAID'
                        elif A.loc[i, 'BKT'] == 1:
                            b = A.loc[i, 'EMI'] + A.loc[i, 'EMI']
                            if B.loc[k, 'AGAINST'] == 'RT':
                                A.loc[i, 'STATUS'] = 'RT'
                            elif B1.loc[j, 'PAID AMOUNT'] >= b:
                                A.loc[i, 'STATUS'] = 'RB'
                            elif (B1.loc[j, 'PAID AMOUNT'] >= A.loc[i, 'EMI']) and (B1.loc[j, 'PAID AMOUNT'] < b):
                                A.loc[i, 'STATUS'] = 'SB'
                            elif B1.loc[j, 'PAID AMOUNT'] < A.loc[i, 'EMI']:
                                A.loc[i, 'STATUS'] = 'PART PAID'
            elif (A.loc[i, 'AGREEMENTID'] == B.loc[k, 'AGREEMENTID']) and (B.loc[k, 'AGAINST'] == 'FORECLOSE'):
                A.loc[i, 'STATUS'] = 'FORECLOSE'
            elif (A.loc[i, 'AGREEMENTID'] == B.loc[k, 'AGREEMENTID']) and (B.loc[k, 'AGAINST'] == 'SETTLEMENT'):
                A.loc[i, 'STATUS'] = 'SETTLEMENT'
    A['STATUS'].fillna('FLOW', inplace=True)
    for i in range(0, len(A['AGREEMENTID'])):
        for j in range(0, len(B1['PAID AMOUNT'])):
            if A.loc[i, 'AGREEMENTID'] == B1.loc[j, 'AGREEMENTID']:
                A.loc[i, 'TOTAL PAID'] = B1.loc[j, 'PAID AMOUNT']

    progress_recorder.set_progress(current=2, total=7)

    M = pd.DataFrame(A.groupby(['COMPANY', 'BKT', 'STATE'])['POS'].sum()).reset_index()

    M.rename({'POS': 'TOTAL_POS'}, axis=1, inplace=True)

    R = pd.DataFrame(A.groupby(['COMPANY', 'BKT', 'STATE'])['AGREEMENTID'].count()).reset_index()

    F = M.merge(R, how='outer')

    F.rename({'AGREEMENTID': 'TOTAL_CASES'}, axis=1, inplace=True)

    R1 = pd.DataFrame(A.groupby(['COMPANY', 'BKT', 'STATE', 'STATUS'])['AGREEMENTID'].count()).reset_index()

    P = F.copy()

    P = P.iloc[:, :3]

    P['FLOW'] = np.nan
    P['SB'] = np.nan
    P['RB'] = np.nan
    P['NM'] = np.nan
    P['PART PAID'] = np.nan
    P['FORECLOSE'] = np.nan
    P['SETTLEMENT'] = np.nan
    P['RT'] = np.nan

    COL = P.columns

    for i in range(0, len(R1['COMPANY'])):
        for j in range(0, len(P['COMPANY'])):
            for k in range(0, len(COL)):
                if ((R1.loc[i, ['COMPANY', 'BKT', 'STATE']] == P.loc[j, ['COMPANY', 'BKT', 'STATE']]).all()) and (
                        R1.loc[i, 'STATUS'] == COL[k]):
                    P.loc[j, COL[k]] = R1.loc[i, 'AGREEMENTID']

    F = F.merge(P, how='outer')

    F.fillna(0, inplace=True)

    F.rename({'FLOW': 'FLOW_CASES', 'SB': 'SB_CASES', 'RB': 'RB_CASES', 'FORECLOSE': 'FORECLOSE_CASES',
              'SETTLEMENT': 'SETTLEMENT_CASES', 'NM': 'NM_CASES', 'PART PAID': 'PART_PAID_CASES', 'RT': 'RT_CASES'},
             axis=1, inplace=True)

    R2 = pd.DataFrame(A.groupby(['COMPANY', 'BKT', 'STATE', 'STATUS'])['POS'].sum()).reset_index()

    for i in range(0, len(R2['COMPANY'])):
        for j in range(0, len(P['COMPANY'])):
            for k in range(0, len(COL)):
                if ((R2.loc[i, ['COMPANY', 'BKT', 'STATE']] == P.loc[j, ['COMPANY', 'BKT', 'STATE']]).all()) and (
                        R2.loc[i, 'STATUS'] == COL[k]):
                    P.loc[j, COL[k]] = R2.loc[i, 'POS']

    F = F.merge(P, how='outer')

    F.rename({'FLOW': 'FLOW_POS', 'SB': 'SB_POS', 'RB': 'RB_POS', 'FORECLOSE': 'FORECLOSE_POS', 'NM': 'NM_POS',
              'SETTLEMENT': 'SETTLEMENT_POS', 'PART PAID': 'PART_PAID_POS', 'RT': 'RT_POS'}, axis=1, inplace=True)

    F.fillna(0, inplace=True)

    for i in range(0, len(F['FLOW_CASES'])):
        F.loc[i, 'FLOW_POS%'] = round((F.loc[i, 'FLOW_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)
        F.loc[i, 'SB_POS%'] = round((F.loc[i, 'SB_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)
        F.loc[i, 'RB_POS%'] = round((F.loc[i, 'RB_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)
        F.loc[i, 'FORECLOSE_POS%'] = round((F.loc[i, 'FORECLOSE_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)
        F.loc[i, 'SETTLEMENT_POS%'] = round((F.loc[i, 'SETTLEMENT_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)
        F.loc[i, 'NM_POS%'] = round((F.loc[i, 'NM_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)
        F.loc[i, 'PART_PAID_POS%'] = round((F.loc[i, 'PART_PAID_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)
        F.loc[i, 'RT_POS%'] = round((F.loc[i, 'RT_POS'] / F.loc[i, 'TOTAL_POS']) * 100, 2)

    progress_recorder.set_progress(current=3, total=7)

    TP = pd.DataFrame(A.groupby(['COMPANY', 'BKT', 'STATE'])['TOTAL PAID'].sum()).reset_index()

    F = F.merge(TP, how='outer')

    for i in range(0, len(F['SB_POS'])):
        F.loc[i, 'PERFORMANCE'] = F.loc[i, 'SB_POS%'] + F.loc[i, 'RB_POS%'] + F.loc[i, 'FORECLOSE_POS%'] + F.loc[
            i, 'NM_POS%'] + F.loc[i, 'SETTLEMENT_POS%'] + F.loc[i, 'RT_POS%']
        F.loc[i, 'Additional_Performance'] = F.loc[i, 'RB_POS%'] + F.loc[i, 'NM_POS%'] + F.loc[i, 'SETTLEMENT_POS%'] + \
                                             F.loc[i, 'FORECLOSE_POS%']

    for i in range(0, len(F['FLOW_CASES'])):
        F.loc[i, 'PERFORMANCE'] = round(F.loc[i, 'PERFORMANCE'], 2)
        F.loc[i, 'Additional_Performance'] = round(F.loc[i, 'Additional_Performance'], 2)

    F.rename({'TOTAL_CASES': 'COUNT', 'PART_PAID_CASES': 'PP_CASES', 'FORECLOSE_CASES': 'FC_CASES',
              'SETTLEMENT_CASES': 'SC_CASES', 'PART_PAID_POS': 'PP_POS', 'FORECLOSE_POS': 'FC_POS',
              'SETTLEMENT_POS': 'SC_POS', 'FORECLOSE_POS%': 'FC_POS%', 'SETTLEMENT_POS%': 'SC_POS%',
              'PART_PAID_POS%': 'PP_POS%', 'PERFORMANCE': 'POS_RES%'}, axis=1, inplace=True)

    progress_recorder.set_progress(current=4, total=7)

    for i in range(0, len(F['FLOW_CASES'])):
        F.loc[i, 'TOTAL_POS'] = round(F.loc[i, 'TOTAL_POS'], 2)
        F.loc[i, 'FLOW_POS'] = round(F.loc[i, 'FLOW_POS'], 2)
        F.loc[i, 'SB_POS'] = round(F.loc[i, 'SB_POS'], 2)
        F.loc[i, 'RB_POS'] = round(F.loc[i, 'RB_POS'], 2)
        F.loc[i, 'NM_POS'] = round(F.loc[i, 'NM_POS'], 2)
        F.loc[i, 'PP_POS'] = round(F.loc[i, 'PP_POS'], 2)
        F.loc[i, 'FC_POS'] = round(F.loc[i, 'FC_POS'], 2)
        F.loc[i, 'SC_POS'] = round(F.loc[i, 'SC_POS'], 2)
        F.loc[i, 'RT_POS'] = round(F.loc[i, 'RT_POS'], 2)
        F.loc[i, 'TOTAL PAID'] = round(F.loc[i, 'TOTAL PAID'], 2)

    progress_recorder.set_progress(current=5, total=7)

    print(F)
    F.to_excel('media/IDFC_TW/MIS/MAR 22/Performance_IDFC_TW.xlsx', index=False)
    F.to_excel('media/IDFC_TW/Billing/MAR 22/Performance_IDFC_TW.xlsx', index=False)
    F1 = F.copy()

    F.to_excel(r'media/IDFC_TW/MIS/MAR 22/MIS_IDFC_TW.xlsx', index=False)

    F.replace(np.nan, 0, inplace=True)

    F.to_excel(r'media/IDFC_TW/Billing/MAR 22/Performance_IDFC_TW.xlsx', index=False)

    progress_recorder.set_progress(current=6, total=7)

    for i in range(0, len(A['AGREEMENTID'])):
        s = 0
        for j in range(0, len(B['AGREEMENTID'])):
            if (A.loc[i, 'AGREEMENTID'] == B.loc[j, 'AGREEMENTID']) and (
                    A.loc[i, 'STATUS'] == 'FORECLOSE' or A.loc[i, 'STATUS'] == 'SETTLEMENT' or A.loc[
                i, 'STATUS'] == 'NM' or A.loc[i, 'STATUS'] == 'RB' or A.loc[i, 'STATUS'] == 'SB') and (
                    B.loc[j, 'MODE'] != 'ECS'):
                s = s + B.loc[j, 'PAID AMOUNT']
        A.loc[i, 'Billing PAID AMT.'] = s
        if A.loc[i, 'STATUS'] == 'SETTLEMENT' or A.loc[i, 'STATUS'] == 'FORECLOSE':
            A.loc[i, 'Billing PAID AMT.'] = s
        elif (A.loc[i, 'BKT'] == 1) and (A.loc[i, 'STATUS'] == 'RB'):
            if A.loc[i, 'Billing PAID AMT.'] > ((A.loc[i, 'EMI']) * 2):
                A.loc[i, 'Billing PAID AMT.'] = A.loc[i, 'EMI'] + A.loc[i, 'EMI']
            else:
                A.loc[i, 'Billing PAID AMT.'] = s
        elif A.loc[i, 'STATUS'] == 'SB':
            if A.loc[i, 'Billing PAID AMT.'] > A.loc[i, 'EMI']:
                A.loc[i, 'Billing PAID AMT.'] = A.loc[i, 'EMI']
            else:
                A.loc[i, 'Billing PAID AMT.'] = s

    A.to_excel(r'media/IDFC_TW/MIS/MAR 22/MASTER FILE IDFC_TW.xlsx', index=False)
    A.to_excel(r'media/IDFC_TW/Billing/MAR 22/MASTER FILE IDFC_TW.xlsx', index=False)
    A.to_excel(r'media/IDFC_TW/TC Performance/MAR 22/MASTER FILE IDFC_TW.xlsx', index=False)
    A.to_excel(r'media/IDFC_TW/FOS Salary/MAR 22/MASTER FILE IDFC_TW.xlsx', index=False)
    A.to_excel(r'media/IDFC_TW/TC Incentive/MAR 22/MASTER FILE IDFC_TW.xlsx', index=False)

    progress_recorder.set_progress(current=7, total=7)