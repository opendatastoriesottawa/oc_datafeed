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
import write_to_text

#load stops
stops = pd.read_csv('D:/Dougall/OpenDataStories/Posts/OC_Transpo/google_transit/stops.txt')

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



#the job to called - creates and appends to a selected list.
#sleeps for a specified period of time
    
def oc_job(xsec, _list, app_id, app_key, acc_url,fpath):
    print("going to sleep for {}".format(xsec))
    time.sleep(xsec)
    #just produce a list of json structures
    _json = get_trip_from_stop_all(random_stop(), app_id, app_key, acc_url)
    write_to_text.write_to_text(fpath,str(_json))
    #_list.append(get_trip_from_stop_all(random_stop(), app_id, app_key, acc_url))
    print("appended to list")
    return schedule.CancelJob  


#Use schedule_next_job functions here
def sched_oc_call(_list, counter, app_id, app_key, acc_url,fpath):
    
    now = datetime.datetime.now()
    midnight = datetime.datetime.combine(now.date(), datetime.time())
    #_list = []
    while True:
        _time = int((now - midnight).seconds)
        next_sleep = sj.next_sleep(counter, _time)
        oc_job(next_sleep, _list, app_id, app_key, acc_url, fpath)
        counter = sj.increase_count(counter)

        if counter == 10000:
            _seconds = _time
            counter = sj.sleep_until_midnight(_seconds, counter)
            counter = sj.reset_counter()
    return



#app_id = "c344d57c"
#app_key = "94eb25476a22333242121acbfe9240ca"
#acc_url = "https://api.octranspo1.com/v1.2/GetNextTripsForStopAllRoutes"
#biglist = []
#sched_oc_call(oc_job, biglist, 5, app_id, app_key, acc_url, 2)
