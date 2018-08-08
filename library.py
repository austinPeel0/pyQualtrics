# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 13:38:44 2017

@author: AustinPeel
"""

import pandas as pd
import re , sqlite3, SQL, os
from pandas.io.json import json_normalize
from qualtrics import version3

def getLastResponse(file):
    lastDF = pd.read_csv(file,encoding="ISO-8859-1")
    last = lastDF.tail(1)
    lastResponse = str(last['ResponseID'].values)
    lastResponse = re.sub('\[' ,"",lastResponse)
    lastResponse = re.sub('\]' ,"",lastResponse)
    lastResponse = re.sub("\'" ,"",lastResponse) 
    return lastResponse

def getLastEndDate(file):
    lastDF = pd.read_csv(file,encoding="ISO-8859-1")
    last = lastDF.tail(1)
    lastResponse = str(last['EndDate'].values)
    lastResponse = re.sub('\[' ,"",lastResponse)
    lastResponse = re.sub('\]' ,"",lastResponse)
    lastResponse = re.sub("\'" ,"",lastResponse) 
    return lastResponse

def getFirstEndDate(file):
    lastDF = pd.read_csv(file,encoding="ISO-8859-1")
    first = lastDF.head(3)
    last = first.tail(1)
    lastResponse = str(last['EndDate'].values)
    lastResponse = re.sub('\[' ,"",lastResponse)
    lastResponse = re.sub('\]' ,"",lastResponse)
    lastResponse = re.sub("\'" ,"",lastResponse) 
    return lastResponse

def getSurveyCounts(file):
    df = pd.read_csv(file,encoding="ISO-8859-1")
    count = len(df.index)
    return count
    
def getDataFrame(location,fileName):
    name= location + '/' + fileName    
    df = pd.read_csv(name)
    df =df.drop(df.index[[0,1]])
    df = df.rename(columns= {list(df)[0]:'ID'})
    wide = pd.melt(df, id_vars="ID")
    #wide = wide[wide['variable'].str.startswith('Q')]
    return wide

def getQuestionLookup(surveyJSON):
    df2 = pd.DataFrame()
    for key in surveyJSON['result']['questions'].keys():
        df = json_normalize(surveyJSON['result']['questions'][key])
        df['QID'] = key
        df2 =df2.append(df)
    cols=['questionText','questionLabel','QID']
    df2 = df2[cols]
    return df2

def getColumnMappings(surveyJSON):
    df2 = pd.DataFrame()
    for key in surveyJSON['result']['exportColumnMap'].keys():
            df3 = json_normalize(surveyJSON['result']['exportColumnMap'][key])
            df3['mapping'] = key
            df2 =df2.append(df3)
    df2 = df2.rename(columns={'question': 'QID'}) 
    return df2

def getQuestionChoices(surveyJSON):
    df2 = pd.DataFrame()
    for key in surveyJSON['result']['questions'].keys():
            df = json_normalize(surveyJSON['result']['questions'][key])
            df['QID'] = key
            df2 =df2.append(df)
    cols=['questionText','questionLabel','QID']
    wide = pd.melt(df2, id_vars=cols)
    wide2 =  wide.dropna(subset=['value']) 
    wide2 = wide2[wide2['variable'].str.endswith('Text')]
    wide2['variable'] = wide2['variable'].str.replace('.choiceText', '')
    wide2['choices'] = wide2['QID'] + "." + wide2['variable']
    cols = ['choices','value']
    final = wide2[cols]
    return final


def getColumnInfo(surveyJSON):
    df = getQuestionLookup(surveyJSON)
    df2 = getColumnMappings(surveyJSON)
    df = pd.merge(df,df2,on="QID",how='left')
    df = df.fillna('')
    df['choices'] =df['choice'] +df['subQuestion']
    del df['choice']
    del df['subQuestion']
    del df['textEntry']
    df2 = getQuestionChoices(surveyJSON)
    df = pd.merge(df,df2,on='choices',how='left')
    return df

def getSurveyInfo(surveyJSON):
    surveyName = surveyJSON['result']['name']
    df = json_normalize(surveyJSON['result']['responseCounts'])
    size = len(surveyJSON['result']['questions'])
    df['size'] = size
    df['surveyName'] = surveyName
    return df 

def surveyToSqlite(sqlDB,folderLocation,surveyID):
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    surveyJSON = version3.qualtrics(survey=surveyID).getSurveyInfo()
    df  = getSurveyInfo(surveyJSON)
    try:
        SQL.main.sendSQLite(df,'surveyInfo',conn)
    except sqlite3.OperationalError:
        SQL.main.creatTableLite(df,'surveyInfo',conn)
        SQL.main.sendSQLite(df,'surveyInfo',conn)
        
def getSurveyName(sqlDB,folderLocation): 
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    sql = 'SELECT surveyName FROM surveyInfo ORDER BY ROWID ASC LIMIT 1'
    name = SQL.main.pullSqlite(sql,conn)
    return str(name[0][0])

def getSurveyDownloadData(file,timeStamp):
    last = getLastResponse(file)
    count = getSurveyCounts(file)
    firstDate = getFirstEndDate(file)
    lastDate = getLastEndDate(file)
    d = {'lastResponse': last, 'responseCount': count,'fromDate':firstDate,'toDate':lastDate,'timeStamp':timeStamp}
    df = pd.DataFrame(data=d,index=[timeStamp])
    return df

def surveyDownloadsToSqlite(sqlDB,file,folderLocation,timeStamp):
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    df  = getSurveyDownloadData(file,timeStamp)
    try:
        SQL.main.sendSQLite(df,'surveyDownload',conn)
    except sqlite3.OperationalError:
        SQL.main.creatTableLite(df,'surveyDownload',conn)
        conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
        SQL.main.sendSQLite(df,'surveyDownload',conn)

def getLastResonseSqlite(sqlDB,folderLocation): 
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    sql = 'SELECT lastResponse FROM surveyDownload ORDER BY ROWID DESC LIMIT 1'
    name = SQL.main.pullSqlite(sql,conn)
    return str(name[0][0])

def getLastTimeStampSqlite(sqlDB,folderLocation): 
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    sql = 'SELECT timeStamp FROM surveyDownload ORDER BY ROWID DESC LIMIT 1'
    name = SQL.main.pullSqlite(sql,conn)
    return str(name[0][0])

def saveMetaDataToCSV(sqlDB,tableName):
    conn =  sqlite3.connect(sqlDB)
    df =  pd.read_sql_query("select * from " + tableName, conn)
    df.to_csv(os.path.dirname(sqlDB)+"/"+tableName+".csv")
    print("csv saved, thank you")
    conn.close()