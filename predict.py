from pymongo import MongoClient
from sklearn.preprocessing import StandardScaler
import datetime
from datetime import date, timedelta
import pickle
import requests
from dateutil.parser import parse
import numpy
def RepresentsFloat(s):
    try: 
        float(s)
        return True
    except ValueError:
        return False
def chValue(s):
    if RepresentsFloat(s):
        v = float(s)
    else:
        v= 4
    return v
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
def inToPA(preData):
    conn=MongoClient('mongodb://admin:admin@cluster0-shard-00-00-9eks9.mongodb.net:27017,cluster0-shard-00-01-9eks9.mongodb.net:27017,cluster0-shard-00-02-9eks9.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true')
    coll3 = conn.fyp.predictData
    preData['dateTime'] = parse(preData['dateTime'].strftime('%Y-%m-%d %H'))
    query = {'dateTime' :preData['dateTime'],'locationCode':preData['locationCode']}
    if(coll3.find(query).count()==0):
        rec=coll3.insert_one(preData)
def genAqhiByL(location):
    conn=MongoClient('mongodb://admin:admin@cluster0-shard-00-00-9eks9.mongodb.net:27017,cluster0-shard-00-01-9eks9.mongodb.net:27017,cluster0-shard-00-02-9eks9.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true')
    collection = conn.fyp.airPollution
    coll2 = conn.fyp.currentAQHI
    lcode = getLocationCode(location)
    if lcode == 17:
        False
    query = { 'dateTime' : {"$lt":datetime.datetime.now(), '$gt':datetime.datetime.now() - timedelta(hours=8)},"locationCode":lcode}
    li2  = []
    li=[]
    for x in collection.find(query).limit(7).sort("dateTime"):
        query = {'time':x['dateTime'],'locationCode':x['locationCode']}
        result=coll2.find_one(query)
        aqhi =3
        if (result):
            aqhi=chValue(result['aqhi'])
     #  1
    t =  datetime.datetime.now() 
    fmt = t.strftime('%Y-%m-%d %H')
    time = parse(fmt)- timedelta(hours=0.5)
    ch =int(t.strftime('%M'))
    if ch > 35:
        time = parse(fmt)+ timedelta(hours=0.5)
    get1 = coll2.find({"locationCode":lcode , 'time':{"$eq":time}}).sort("time",-1).limit(1)
    pre1=3
    for b in get1:
        pre1 = b['aqhi']
    preData = {"dateTime":datetime.datetime.now() + timedelta(hours=1),"location":location,
                        "locationCode":lcode,"paqhi":str(pre1)}
    #  
    print(preData)
    inToPA(preData)
    #   2  
    t =  datetime.datetime.now() 
    fmt = t.strftime('%Y-%m-%d %H')
    time = parse(fmt)- timedelta(hours=1.5)
    ch =int(t.strftime('%M'))
    get1 = coll2.find({"locationCode":lcode , 'time':{"$eq":time}}).sort("time",-1).limit(1)
    pre2=3
    for b in get1:
        pre2 = b['aqhi']
    #         
    preData = {"dateTime":datetime.datetime.now() + timedelta(hours=2),"location":location,
                    "locationCode":lcode,"paqhi":str(pre2)}
    print(preData)
    inToPA(preData)
#            3 
    t =  datetime.datetime.now() 
    fmt = t.strftime('%Y-%m-%d %H')
    time = parse(fmt)- timedelta(hours=2.5)
    ch =int(t.strftime('%M'))
    get1 = coll2.find({"locationCode":lcode , 'time':{"$eq":time}}).sort("time",-1).limit(1)
    pre3=3
    for b in get1:
        pre3 = b['aqhi']
    #  
    preData = {"dateTime":datetime.datetime.now() + timedelta(hours=3),"location":location,
                    "locationCode":lcode,"paqhi":str(pre3)}
    print(preData)
    inToPA(preData)
def genData():
    location = ['Central/western','Eastern','Kwun Tong','Sham Shui Po',
                'Kwai Chung','Tsuen Wan','Tseung Kwan O','Yuen Long',
                'Tuen Mun','Tung Chung','Tai Po','Sha Tin',
                'Tap Mun','Causeway Bay','Central','Mong Kok'] 
    for x in location:
        genAqhiByL(x)
list1 = []
genData()
print(1)
