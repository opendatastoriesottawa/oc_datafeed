# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 19:24:49 2018

OC_API_Functions
Calls to the API, return of the data

@author: douga
"""
import requests
import json
import schedule
import time    
import random
import pandas as pd
import schedule_next_job as sj
import datetime
#import write_to_text
import read_json as rj
import mysql_connection as mysql

#load stops
#stops = pd.read_csv('D:/Dougall/OpenDataStories/Posts/OC_Transpo/google_transit/stops.txt')
stops_tway = pd.read_csv('/odscoll/oc_datafeed/oc_datafeed/stops.txt')

#Function that outputs a JSON dict after making the call to the OC Transpo api
def get_trip_from_stop(route, stop, app_id, app_key, acc_url):
    _data={"appID": app_id,  "apiKey": app_key, "routeNo":route,  "stopNo":stop, "format": "json"}
    json_list = requests.post(acc_url, _data).text
    _json = json.loads(json_list)
    return _json

def get_trip_from_stop_all(stop, app_id, app_key, acc_url):
    
    data={"appID": app_id,  "apiKey": app_key, "stopNo":stop, "format": "json"}
    json_list = requests.post(acc_url, data).text
    _json = json.loads(json_list)
    return _json

#Function to extract the necessary info into a list object
#built this way because the chances it will change are low
def list_trip_data_fromone(_json):
    
    _list = []
    
    #values to add - route, direction, starttime[0], adjschedtime[0], adjage[0], bustype[0]
    _list.append(_json['GetNextTripsForStopResult']['Route']['RouteDirection']['RouteNo'])
    _list.append(_json['GetNextTripsForStopResult']['Route']['RouteDirection']['Direction'])
    _list.append(_json['GetNextTripsForStopResult']['Route']['RouteDirection']['Trips']['Trip'][0]['TripStartTime'])
    _list.append(_json['GetNextTripsForStopResult']['Route']['RouteDirection']['Trips']['Trip'][0]['AdjustedScheduleTime'])
    _list.append(_json['GetNextTripsForStopResult']['Route']['RouteDirection']['Trips']['Trip'][0]['AdjustmentAge'])
    _list.append(_json['GetNextTripsForStopResult']['Route']['RouteDirection']['Trips']['Trip'][0]['BusType'])
    
    
    return _list


#random function to take in a list of stops and output a random stop
#a list of stops. will randomly select and ping them


def random_stop():
    
    _stop = random.choice(stops['stop_code'].dropna().tolist())
    return _stop


#collecting data on transitway stops
def transitway_stop(idx):
    stop = stops_tway.at[idx, 'stop_code']
    return stop
    
    


#the job to called - creates and appends to a selected list.
#sleeps for a specified period of time
    
def oc_job(xsec, _list, app_id, app_key, acc_url,fpath, mysqlkeys, idx):
    print("going to sleep for {}".format(xsec))
    time.sleep(xsec)
    
    #timing
    #just produce a list of json structures
    try:
        #_json = get_trip_from_stop_all(random_stop(), app_id, app_key, acc_url)
        _json = get_trip_from_stop_all(transitway_stop(idx), app_id, app_key, acc_url)
    except:
        print("failed to load stop json")
        _json = []
    
    
    #write_to_text.write_to_text(fpath,str(_json))
    try:
        _dict = rj.extract_json(_json)
    except:
        print("failed to extract json")
        _dict = {}
    try:
        mysql.data_to_db(_dict, mysqlkeys)  
    except:
        print('failed to write to db')
    #_list.append(get_trip_from_stop_all(random_stop(), app_id, app_key, acc_url))
    return schedule.CancelJob  


#Use schedule_next_job functions here
def sched_oc_call(_list, counter, app_id, app_key, acc_url,fpath, mysqlkeys):
    counter2 = counter
    idx = 0 #index for transitway stops
    now = datetime.datetime.now()
    midnight = datetime.datetime.combine(now.date(), datetime.time())
    _time = int((now - midnight).seconds)

    while True:
        start = time.time()
        #next_sleep = sj.next_sleep(counter, _time)
        next_sleep = 2 #for transitway, set time between calls to 9 seconds - found that with all db connections and stuff, this takes much longer than the amount of time specified
        if idx >= len(stops_tway): #reset when larger than index
            idx = 0
        oc_job(next_sleep, _list, app_id, app_key, acc_url, fpath, mysqlkeys,idx)
        idx += 1
    	
	#update the counter, then check the count
        
        try:
            mysql.counter(mysqlkeys)
            counter = mysql.get_counter(mysqlkeys)
            print("counter  tried. counter at {}".format(counter))
        except:
            print('counter hit exception. not adding to counter db')
            counter2 = sj.increase_count(counter)

        if counter == 9000:
            print('counter at 90%')
        try:
            xxx = counter2 * 1
        except:
            counter2 = 0
        if counter2 == 9000:
            print('counter at 90%')
        now = datetime.datetime.now()
        _time = int((now - midnight).seconds)
       
        if counter == 10000:
            print('counter hit 10000')
            now = datetime.datetime.now()
            midnight = datetime.datetime.combine(now.date(), datetime.time())
            _time = int((now - midnight).seconds)
            counter = sj.sleep_until_midnight(_time, counter)
            counter = sj.reset_counter()
        
        end = time.time()
        timer = end - start
        print("Run took {} seconds".format(timer))

       # THIS SHOULD NO LONGER BE NEEDED - SQL WILL RESET COUNTER AT START OF NEW DAY    
        # now = datetime.datetime.now()
        # midnight = datetime.datetime.combine(now.date(), datetime.time())
        # _time = int((now - midnight).seconds)         #deal with midnight
        # if _time > 0 and _time < 36 and counter > 4:
        #     counter = sj.reset_counter()
               
        
        
    return



#app_id = "c344d57c"
#app_key = "94eb25476a22333242121acbfe9240ca"
#acc_url = "https://api.octranspo1.com/v1.2/GetNextTripsForStopAllRoutes"
#biglist = []
#sched_oc_call(oc_job, biglist, 5, app_id, app_key, acc_url, 2)
