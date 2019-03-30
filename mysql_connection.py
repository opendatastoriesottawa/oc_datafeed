# -*- coding: utf-8 -*-
"""
Created on Fri Feb  8 18:42:17 2019

Connection to a MySQL database

@author: douga
"""
import pymysql.cursors

from datetime import datetime

dt = datetime.now()




def __mysql_connect(_dict, mysqlkeys):
    if len(_dict) == 0:
        return
    #setup each variable here
    try:
        _datetime = _dict['datetime']
    except:
        _datetime = None
    
    _error = _dict['Error']
    
    StopNo = None
    try:
        if _dict['StopNo'] is not None and _dict['StopNo'] is not '':
            StopNo = int(_dict['StopNo'])
    except:
        StopNo = None
    
    try:
        StopDescription = _dict['StopDescription']
    except:
        StopDescription = None
    
    try:
        Direction = _dict['Direction']
    except:
        Direction = None
    
    DirectionID = None
    try:
        if _dict['DirectionID'] is not None and _dict['DirectionID'] is not '':
            DirectionID = int(_dict['DirectionID'])
    except:
        DirectionID = None
    
    try:    
        RouteHeading = _dict['RouteHeading']
    except:
        RouteHeading = None
    
    RouteNo = None
    try:
        if _dict['RouteNo'] is not None and _dict['RouteNo'] is not '':
            RouteNo = int(_dict['RouteNo'])
    except:
        RouteNo = None
        
    AdjustedScheduleTime = None
    try:
        if _dict['AdjustedScheduleTime'] is not None and _dict['AdjustedScheduleTime'] is not '':
            AdjustedScheduleTime = int(_dict['AdjustedScheduleTime'])
    except:
        AdjustedScheduleTime = None
     
    AdjustmentAge = None
    try:
        if _dict['AdjustmentAge'] is not None and _dict['AdjustmentAge'] is not '':
            AdjustmentAge = float(_dict['AdjustmentAge'])
    except:
        AdjustmentAge = None
    try:    
        BusType = _dict['BusType']
    except:
        BusType = None
    
    GPSSpeed = None
    try:
        if _dict['GPSSpeed'] is not None and _dict['GPSSpeed'] is not '':
            GPSSpeed = float(_dict['GPSSpeed'])
    except:
            GPSSpeed = None
            
    try:
        LastTripOfSchedule = _dict['LastTripOfSchedule']
    except:
        LastTripOfSchedule = None
    
    Latitude = None
    try:
        if _dict['Latitude'] is not None and _dict['Latitude'] is not '':
            Latitude = float(_dict['Latitude'])
    except:
        Latitude = None
    
    Longitude = None
    try:
        if _dict['Longitude'] is not None and _dict['Longitude'] is not '':
            Longitude = float(_dict['Longitude'])
    except:
        Longitude = None
    
    try:    
        TripDestination = _dict['TripDestination']
    except:
        TripDestination = None
    
    TripStartTime = None
    try:
        if _dict['TripStartTime'] is not None and _dict['TripStartTime'] is not '':
            TripStartTime = datetime.strptime(_dict['TripStartTime'], '%H:%M').time()
    except:
        TripStartTime = None
    
    
    
    # Connect to the database
    connection = pymysql.connect(host=mysqlkeys['host'],
                                 user=mysqlkeys['user'],
                                 password=mysqlkeys['pass'],
                                 db='oc_datafeed',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO trips_tway (_datetime, _error, StopNo, StopDescription, Direction, DirectionID, RouteHeading, RouteNo, AdjustedScheduleTime, AdjustmentAge, BusType, GPSSpeed, LastTripOfSchedule, Latitude, Longitude, TripDestination, TripStartTime, expSet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (_datetime, _error, StopNo, StopDescription, Direction, DirectionID, RouteHeading, RouteNo, AdjustedScheduleTime, AdjustmentAge, BusType, GPSSpeed, LastTripOfSchedule, Latitude, Longitude, TripDestination, TripStartTime, False)
            cursor.execute(sql, values)
            #make updates to the table
            upd1 = """UPDATE trips_tway SET expectedTime = _datetime + INTERVAL AdjustedScheduleTime MINUTE WHERE expSet=FALSE"""
            upd2 = """UPDATE trips_tway SET expectedTime_nd = TIME(expectedTime) WHERE expSet=FALSE"""
            upd3 = """UPDATE trips_tway SET expectedTime_ndh = HOUR(expectedTime_nd) WHERE expSet=FALSE"""
            upd4 = """UPDATE trips_tway SET expectedTime_ndm = MINUTE(expectedTime_nd) WHERE expSet=FALSE"""
            upd5 = """ UPDATE trips_tway SET expSet=TRUE """
            cursor.execute(upd1)
            cursor.execute(upd2)
            cursor.execute(upd3)
            cursor.execute(upd4)
            cursor.execute(upd5)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

    #    with connection.cursor() as cursor:
    #        # Read a single record
    #        sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
    #        cursor.execute(sql, ('webmaster@python.org',))
    #        result = cursor.fetchone()
    #        print(result)
    finally:
        connection.close()
        
    return


def data_to_db(_listdict, mysqlkeys):
    for _dict in _listdict:
        __mysql_connect(_dict, mysqlkeys)
        
    return
    
    
    


  
    
#COLUMNS:
"""
LOADID
datetime
error
StopNo
StopDescription
Direction
DirectionID
RouteHeading
RouteNo
AdjustedScheduleTime
AdjustmentAge
BusType
GPSSpeed
LastTripOfSchedule
Latitude
Longitude
TripDestination
TripStartTime
"""