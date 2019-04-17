from dateutil.parser import parse
from datetime import datetime
import pandas as pd
import numpy as np
import hashlib
import time
import datetime
from datetime import date, timedelta
import requests, re, json
import bs4
from bs4 import BeautifulSoup
import pymongo
from pymongo import MongoClient   
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
def getLocationCode(location):
    locaList=[['causewaybay',0],['central',1],['central/western',2],['eastern',3]
             ,['kwaichung',4],['kwuntong',5],['mongkok',6],['shamshuipo',7]
             ,['shatin',8],['',9],['taipo',10],['tapmun',11],['tseungkwano',12]
             ,['tsuenwan',13],['tuenmun',14],['tungchung',15],['yuenlong',16]]
    location = location.replace(' ','').casefold()
    for value in locaList:
        if value[0]==location:
            return value[1]
    return 17
@sched.scheduled_job('interval', minutes=30)
def job1():
    print ("%s: job"  % time.asctime())
    df = pd.DataFrame(columns=['dateTime','location','NO2','O3','SO2','CO','PM10','PM2.5'])
    web = ['http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration45fd.html?stationid=80',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentratione1a6.html?stationid=73',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentrationfb71.html?stationid=74',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentrationdb46.html?stationid=66',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration30e8.html?stationid=72',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration228e.html?stationid=77',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration0b35.html?stationid=83',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration1f2c.html?stationid=70',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration537c.html?stationid=82',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentrationf322.html?stationid=78',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration6e9c.html?stationid=69',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration2c5f.html?stationid=75',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration233a.html?stationid=76',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration5ca5.html?stationid=71',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentrationf9dd.html?stationid=79',
           'http://www.aqhi.gov.hk/en/aqhi/past-24-hours-pollutant-concentration9c57.html?stationid=81']
    
    location = ['Central/western','Eastern','Kwun Tong','Sham Shui Po',
                'Kwai Chung','Tsuen Wan','Tseung Kwan O','Yuen Long',
                'Tuen Mun','Tung Chung','Tai Po','Sha Tin',
                'Tap Mun','Causeway Bay','Central','Mong Kok']  
    for url,i in zip(web,location):  
        res = requests.get(url)
        html = requests.get(url).text.encode('utf-8-sig')
        md5 = hashlib.md5(html).hexdigest()
        if '/' in i: 
            path = './data2/'+i.split('/')[0]+i.split('/')[1]+'.txt'
        else:
            path = './data2/'+i+'.txt'
       
        if False:
            print('Unchanged data ~')
        else:      
            sp = BeautifulSoup(res.text, 'html.parser')
            data = sp.find_all('td',{'class':'H24C_ColDateTime'})
            data1= sp.find_all('td',{'class':'H24C_ColItem'})
            dateTime = [str(dt.text) for dt in data]
            dateTime = [re.sub('\xa0',' ',i) for i in dateTime]
            data1 = np.array([str(dt.text) for dt in data1]).reshape(int(len(data1)/6),6)
            df1 = pd.DataFrame(data1,columns=['NO2','O3','SO2','CO','PM10','PM2.5'])
            df1['dateTime'] = dateTime
            df1['location'] = i
            df1 = df1.rename(columns={'PM2.5': 'PM25'})
            client = MongoClient('mongodb://admin:admin@cluster0-shard-00-00-9eks9.mongodb.net:27017,cluster0-shard-00-01-9eks9.mongodb.net:27017,cluster0-shard-00-02-9eks9.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true')
            db = client['fyp']    
            collection = db.airPollution
            l1 = []
            for i in range(0,len(df1)):
                l2={ 'dateTime':parse(df1['dateTime'][i]),
                    'location':df1['location'][i],
                    'locationCode':getLocationCode(df1['location'][i]),
                    'NO2':df1['NO2'][i],
                    'O3':df1['O3'][i],
                    'SO2':df1['SO2'][i],
                    'CO':df1['CO'][i],
                    'PM10':df1['PM10'][i],
                    'PM25':df1['PM25'][i]
                }
                query = {'dateTime':parse(df1['dateTime'][i]),'locationCode':getLocationCode(df1['location'][i])}
                if (collection.find(query).count()==0):
                    inputdata = collection.insert_one(l2)
                l1.append(l2)
@sched.scheduled_job('interval', minutes=30)
def job2():
    res=requests.get('http://www.aqhi.gov.hk/epd/ddata/html/out/aqhi_ind_rss_Eng.xml')
    html = requests.get('http://www.aqhi.gov.hk/epd/ddata/html/out/aqhi_ind_rss_Eng.xml').text.encode('utf-8-sig')
    md5 = hashlib.md5(html).hexdigest()

    if False:
        print('Unchanged data ~')
    else:
        # type(res)
        # res.text
        soup = bs4.BeautifulSoup(res.text,'lxml')
        # type(soup)
        aqhi = soup.select('title')
        location= soup.select('pubdate')
        for x in range(2,len(aqhi)):
            y=[int(s) for s in aqhi[x].getText().split() if s.isdigit()]
#             conn=MongoClient('mongodb+srv://admin:admin@cluster0-fstcx.mongodb.net/test?retryWrites=true')
            conn=MongoClient('mongodb://admin:admin@cluster0-shard-00-00-9eks9.mongodb.net:27017,cluster0-shard-00-01-9eks9.mongodb.net:27017,cluster0-shard-00-02-9eks9.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true')
            collection=conn.fyp.currentAQHI
            emp_rec={
                "time": parse(location[0].getText().split(' +0800')[0].split(',')[1]),
                "location":aqhi[x].getText().split(':')[0],
                "aqhi":aqhi[x].getText().split(':')[1],
                "locationCode":getLocationCode(aqhi[x].getText().split(':')[0])
            }
            print(location[0].getText())
            print(y)
            print(aqhi[x].getText())
            # Insert Data
            query = {'time':parse(location[0].getText().split(' +0800')[0].split(',')[1]),
                     'locationCode':getLocationCode(aqhi[x].getText().split(':')[0])}
            if (collection.find(query).count()==0):
                rec=collection.insert_one(emp_rec)
            #print data saved
            cursor=collection.find()
            for record in cursor:
                print(record)
job1()
job2()
sched.start()
# scheduler.add_job(job1, 'interval', minutes=30 )
# scheduler.add_job(job2, 'interval', minutes=30 )
# scheduler.start()