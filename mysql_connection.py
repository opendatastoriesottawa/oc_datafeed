# -*- coding: utf-8 -*-
"""
Created on Fri Feb  8 18:42:17 2019

Connection to a MySQL database

@author: douga
"""
import pymysql.cursors

from datetime import datetime, date, timedelta
import time


dt = datetime.now()




def __mysql_connect(_listdict, mysqlkeys):

    
    #use these empty lists to populate sql commands/values
    sqlcomms = []
    sqlvals = []

    for _dict in _listdict:

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

		#if datetime object, we can now build the other datetime objects
        if _datetime != None and AdjustedScheduleTime != None:
            _expTime = _datetime + timedelta(minutes=AdjustedScheduleTime)
            _expTime_nd = _expTime.strftime('%H:%M:%S')
            _expTime_ndh = _expTime.hour
            _expTime_ndm = _expTime.minute
        else:
            _expTime = None
            _expTime_nd = None
            _expTime_ndh = None
            _expTime_ndm = None
			
			
        #create the mysql commands here in a list
        #INSERT INTO trips
        sql = "(_datetime, _error, StopNo, StopDescription, Direction, DirectionID, RouteHeading, RouteNo, AdjustedScheduleTime, AdjustmentAge, BusType, GPSSpeed, LastTripOfSchedule, Latitude, Longitude, TripDestination, TripStartTime, expectedTime, expectedTime_nd, expectedTime_ndh, expectedTime_ndm) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (_datetime, _error, StopNo, StopDescription, Direction, DirectionID, RouteHeading, RouteNo, AdjustedScheduleTime, AdjustmentAge, BusType, GPSSpeed, LastTripOfSchedule, Latitude, Longitude, TripDestination, TripStartTime, _expTime, _expTime_nd, _expTime_ndh, _expTime_ndm)
        
        sqlcomms.append(sql)
        sqlvals.append(values)
    
    upd0 = ""
    for j in range(len(sqlcomms)):
       if j < len(sqlcomms):
           sqlcomms[j] = "{},".format(sqlcomms[j])
       upd0 = "{} {}".format(upd0, sqlcomms[j])

    #finish writing upd0
    upd0 = "INSERT INTO trips {}".format(sql)
    
    # Connect to the database
    connection = pymysql.connect(host=mysqlkeys['host'],
                                 user=mysqlkeys['user'],
                                 password=mysqlkeys['pass'],
                                 db='oc_datafeed',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    #try:
    with connection.cursor() as cursor:
            # Create new records
        cursor.executemany(upd0, sqlvals)
            #make updates to the table
            # upd1 = """UPDATE trips SET expectedTime = _datetime + INTERVAL AdjustedScheduleTime MINUTE WHERE expSet=FALSE"""
            # upd2 = """UPDATE trips SET expectedTime_nd = TIME(expectedTime) WHERE expSet=FALSE"""
            # upd3 = """UPDATE trips SET expectedTime_ndh = HOUR(expectedTime_nd) WHERE expSet=FALSE"""
            # upd4 = """UPDATE trips SET expectedTime_ndm = MINUTE(expectedTime_nd) WHERE expSet=FALSE"""
            # upd5 = """ UPDATE trips SET expSet=TRUE where expSet=FALSE """
            # cursor.execute(upd1)
            # cursor.execute(upd2)
            # cursor.execute(upd3)
            # cursor.execute(upd4)
            # cursor.execute(upd5)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
    connection.commit()

    #    with connection.cursor() as cursor:
    #        # Read a single record
    #        sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
    #        cursor.execute(sql, ('webmaster@python.org',))
    #        result = cursor.fetchone()
    #        print(result)
    #except:
        #print("Exception raised in mysql call")
    #finally:
    connection.close()
        
    
    return


def data_to_db(_listdict, mysqlkeys):
    #for _dict in _listdict:
    # removed this section - mysql connections were taking too long in a loop. will instead add all lines in same cursor
    __mysql_connect(_listdict, mysqlkeys)
        
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


#create a connection to the counter table
#rather than counting within the python program, gonna do all the counting from the db
def counter(mysqlkeys):
	#got todays date
	today = str(date.today())
	#check if the mysql table counter has a row with todays date. if not, add with count of 0
	connection = pymysql.connect(host=mysqlkeys['host'],
	user=mysqlkeys['user'],
	password=mysqlkeys['pass'],
	db='oc_datafeed',
	charset='utf8mb4',
	cursorclass=pymysql.cursors.DictCursor)

	with connection.cursor() as cursor:
		sql = 'SELECT MAX(countdate) FROM counter'
		sql2 = 'SELECT MAX(countid) FROM counter'
		cursor.execute(sql)
		
		output = cursor.fetchall()
		
		cursor.execute(sql2)
		cid = cursor.fetchall()
		for k, v in cid[0].items():
			curcid = v
			newcid = v + 1

		for k, v in output[0].items():
			if v is not None:
				sqldate = v.date()
			else:
				sqldate = None
		if str(sqldate) == str(today):
			sql = 'UPDATE counter SET count = count + 1 WHERE countid=%s'
			cursor.execute(sql, curcid)
		
		
		else:
			sql = 'INSERT INTO counter (countid, countdate, count) VALUES(%s, %s, %s)'
			values = (newcid, today, 1)
			cursor.execute(sql, values)


	connection.commit()
	connection.close()


	return



def get_counter(mysqlkeys):
	
	#extract counter value from the counter table
	connection = pymysql.connect(host=mysqlkeys['host'],
	user=mysqlkeys['user'],
	password=mysqlkeys['pass'],
	db='oc_datafeed',
	charset='utf8mb4',
	cursorclass=pymysql.cursors.DictCursor)

	with connection.cursor() as cursor:
		sql = 'SELECT MAX(countdate) FROM counter'
		cursor.execute(sql)
		output = cursor.fetchall()
		for k, v in output[0].items():
			sqldate = v.date()
		
		sql = 'SELECT count FROM counter WHERE countdate=%s'
		cursor.execute(sql, sqldate)
		
		count = cursor.fetchall()

	connection.commit()
	connection.close()


	return count
