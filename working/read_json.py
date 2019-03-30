"""
read the json output
"""

#load the file
import json
from datetime import datetime

#main function - create viable db rows based on extractions
def extract_json(json):
    
    stop = __stop_level(json) #this becomes part of our key - this is the beginning of the database structure
    routes = __route_level(json) #get information at the route level - temporary dataset to be used in trip_level
    trips = __trip_level(routes)
    
    stop_all = []
    for trip in trips:
        
        stop_all.append(__mergedicts(stop, trip))
        
    return stop_all

#function to load json from a txt file. only temporary, as main function being designed to load API call directly to a db
def __load_json_fromtxt(inputfile):
    with open(inputfile, encoding='utf-8') as f:
        _json = json.loads(f.read())
    return _json

#return a dict containiing the current date and time
def __date_time():
    return datetime.now()    
   
#functions we can use to do certain verification
def __isdict(input):
    return isinstance(input, dict)
    
def __islist(input):
    return isinstance(input, list)
    
def __istuple(input):
    return isinstance(input, tuple)
    
#if the input is empty - input a dict or list and check if the length is 0
def __isempty(input):
    if len(input) == 0:
        return True
    else:
        return False
        
#pass in a key (string) and input (dict) and verify if they key is in the dict    
def __keyexists(key, input):
    if key in input:
        return True
    else:
        return False
        
def __mergedicts(dict1, dict2):
    return {**dict1, **dict2}
        
#functions that are specific to the API return

#returns a route dict, but empty
def __emptyroute():
    return {'RouteNo':None,'DirectionID':None,'Direction':None,'RouteHeading':None}
    
#returns a trip dict, but empty
def __emptytrip():
    return {'AdjustedScheduleTime':None, 'AdjustmentAge':None, 'BusType':None, 'GPSSpeed':None, 'LastTripOfSchedule':None, 'Latitude':None, 'Longitude':None, 'TripDestination':None,'TripStartTime':None}


    
    
    
    
def __stop_level(json): #extract the stop level information to a dict
    _dict = {}
    key = 'GetRouteSummaryForStopResult'
    for item in json[key].items():
        if isinstance(item[1], dict):
            continue
        else:
            _dict[item[0]] = item[1]
    _dict['datetime'] = __date_time()   
    return _dict
    
    
#returns a list of dictionaries (or a list with a single dictionary). each dictionary has route level information (which means they may have trip level ifnromation inside)
def __route_level(json): #extract route level information to a list of dicts - if only 1, the list will only have 1 instance
    key1 = 'GetRouteSummaryForStopResult'
    key2 = json[key1]['Routes'] #should be a dict object
    key3 = key2['Route'] #list or dict object if only 1
    _listdict = []
    #0 routes
    if __isempty(key2):
        _listdict.append(__emptyroute())
        return _listdict
    
    #1 route
    if __isdict(key3):
        _listdict.append(key3)
        return _listdict
    #2+ routes
    if __islist(key3):
        for item in key3: #each item is one route - dict
            _listdict.append(item)
        return _listdict

#produce a finalized dict of route and trip objects - takes in the route level list of dictionaries, rather than the json object
def __trip_level(inputlist):
    
    key1 = 'Trips'
    trip_list = []
    for item in inputlist: # 1 to many
        _route_dict = {'RouteNo':item['RouteNo'],'DirectionID':item['DirectionID'],'Direction':item['Direction'],'RouteHeading':item['RouteHeading']}
        #no trips in dict
        if not __keyexists(key1, item):
            _trip_dict = __emptytrip()
            _route_trip_dict = __mergedicts(_route_dict, _trip_dict)
            trip_list.append(_route_trip_dict)
            continue
        #trips in dict but no trips    
        if __isempty(item[key1]):
            _trip_dict = __emptytrip()
            _route_trip_dict = __mergedicts(_route_dict, _trip_dict)
            trip_list.append(_route_trip_dict)
            continue
        
        #trips in dict
        else:
            #sometimes, trips leads to a nested dict under trip. other times, trips is a list of dicts
            if 'Trip' in item[key1]:
                if __islist(item[key1]['Trip']):
                    for trip in item[key1]['Trip']:
                        route_trip_dict = __mergedicts(_route_dict, trip)
                        trip_list.append(route_trip_dict)
                else:
                    route_trip_dict = __mergedicts(_route_dict, item[key1]['Trip'])
                    trip_list.append(route_trip_dict)

            if __islist(item[key1]):
                for trip in item[key1]:
                    route_trip_dict = __mergedicts(_route_dict, trip)
                    trip_list.append(route_trip_dict)
                    
            else:
                route_trip_dict = __mergedicts(_route_dict, item[key1])
                trip_list.append(route_trip_dict)


    return trip_list
    
#data = __load_json(r'C:\Users\percido\Documents\PythonScripts\json.txt')
#extract_json(data)
