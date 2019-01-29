# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 20:26:59 2018

extraction of things from json output to flatter structure that could load to a db

GET ROUTE SUMMARY FOR STOP structure
-GetRouteSummaryForStopResult
-Routes (dict)
-Route (dict)
-Trips (dict)
-Trip (list of dicts)

@author: douga
"""

#TEST USING BIG_LIST FROM MAIN

#TEST function to extract from dict / list


#uses the getroutesummaryforstop set up

#dict = getroutesummaryforstopresult
#list setup
#Dict - 1 elem
#Dict - 4 elem
#Dict - 1 elem (Routes)
#List - 2 elem (Route) - could have N elems
#listitemn - 5 elems (route list item)
#list - 3 elems (Trips - next 3 trips)
#listitemn - 9 elems (trrip values, lowest level)

#GetRouteSummaryForStopResult - Error, Routes, StopDescription, StopNo
#Routes - [Route]
#Route[i] - Direction, DirectionID, RouteHeading, RouteNo, [Trips]
#trips[i] - AdjustedScheduleTime, AdjustmentAge, BusType, GPSSpeed, LastTripOfSchedule, LAtitude, Longitude
#TripDestination, TripStartTime


#returns a dict containing the top level information
def stop_summary(_json):
    if _json is None:
        return None
    #if returns error
    if 'Error' in _json:
        return 'Error'
    _key = 'GetRouteSummaryForStopResult'
    _inkeylist = ['StopNo', 'StopDescription', 'Error']
    val = {}
    for i in range(len(_inkeylist)):
        val[_inkeylist[i]] = _json[_key][_inkeylist[i]]
    
    return val

#within trips (dict of dicts, get trip(list of dicts))
def trip(uplevel):
    if 'Trips' in uplevel:
        _key = ['AdjustedScheduleTime', 'AdjustmentAge', 'BusType', 'GPSSpeed', 'LastTripOfSchedule', 'Latitude', 'Longitude', 'TripDestination','TripStartTime']
        _dict = {}
        _trips = []
        #if blank list
        if len(uplevel['Trips']) == 0:
            return None
        #if trip in trips
        if 'Trip' in uplevel['Trips']:
            #if dictionary instead of list
            if isinstance(uplevel['Trips']['Trip'],dict):
                for key in _key:
                    _dict[key] = uplevel['Trips']['Trip'][key]
                return [_dict]
            else:
                for i in range(len(uplevel['Trips']['Trip'])):
                    for key in _key:
                        _dict[key] = uplevel['Trips']['Trip'][i][key]
                    _trips.append(_dict)
                    _dict = {}
                
            return _trips

        #trip not in trips
        else:
            #if no list
            if isinstance(uplevel['Trips'],dict):
                for key in _key:
                    _dict[key] = uplevel['Trips'][key]
                return [_dict]
            #list of dicts containing specified keys
            for i in range(len(uplevel['Trips'])):
                for key in _key:
                     _dict[key] = uplevel['Trips'][i][key]
                _trips.append(_dict)
            return _trips
    

#within routes (dict of dicts, extract more info)
def route(_json):
    
    #if empty
    if len(_json['GetRouteSummaryForStopResult']['Routes']) == 0:
        return None, None
    
    _maindict = _json['GetRouteSummaryForStopResult']['Routes']['Route']
    _key = ['Direction', 'DirectionID', 'RouteHeading', 'RouteNo']
    _routeval = []
    _routeval2 = {}
    
    
    if len(_maindict) == 0:
        return None, None

    if isinstance(_maindict,dict):
        for key in _key:
            print(_maindict[key])
            _routeval2[key] = _maindict[key]
        _routeval.append(_routeval2)
        _trips = trip(_maindict)        
    else:
        for i in range(len(_maindict)):
            for key in _key:
           
                _routeval2[key] = _maindict[i][key]
            
            _routeval.append(_routeval2)
            _routeval2 = {}
            _trips = trip(_maindict[i])
        
    return _routeval, _trips






#TESTING THAT IT WORKS
#print(stop_summary(big_list[0]))
#print(big_list[0])
#routes, trips = route(big_list[1])
#for i in range(len(big_list)):
#    print('*'*25)
#    print('*'*25)
#    print('*'*25)
#    print(i)
#    print(stop_summary(big_list[i]))
#    if stop_summary(big_list[i]) is not None and stop_summary(big_list[i]) != 'Error':
#        routes, trips = route(big_list[i])
#        print(routes)
#        print(trips)

"""
NEED TO REFACTOR HOW WE PULL THIS INFO - SUMMARY< ROUTE AND TRIP SHOULD ALL GET COMBINED INTO 1 LINE PER TRIP
use stop 3011 - Tunneys Pasture to get a good example of a large list
once we have stop summary, should output lines for each trip (route info, trip info on same line)
"""
def route_trip(_json, stopsum):
    #if empty
    if len(_json['GetRouteSummaryForStopResult']['Routes']) == 0:
        return "Empty"
    #otherwise continue
    _maindict = _json['GetRouteSummaryForStopResult']['Routes']['Route'] #access main route object
    _key = ['Direction', 'DirectionID', 'RouteHeading', 'RouteNo'] #keys within route
    
    route = {} #dictionary to contain all info for specific route

    #if route is singular, dict
    if isinstance(_maindict,dict):
        for key in _key:
            route[key] = _maindict[key]
        trip_list = set_trip(stopsum, route, _maindict)
    #if many routes at stop, list
    else:
        trip_list = []
        for i in range(len(_maindict)):
            for key in _key:
                route[key] = _maindict[i][key]
            trip_list.append(set_trip(stopsum, route, _maindict[i]))
            route = {}
        
    return trip_list

#takes in a dict from the upper level route key, returns a list of trip dicts if available
#also takes in the path of the level immediately above
def set_trip(stopsum, route_dict, uplevel):
    _key = ['AdjustedScheduleTime', 'AdjustmentAge', 'BusType', 'GPSSpeed', 'LastTripOfSchedule', 'Latitude', 'Longitude', 'TripDestination','TripStartTime']
    trip = {}
    _triplist = []
    
    if 'Trips' in uplevel:
        if len(uplevel['Trips']) == 0:
            return [route_dict,{'Empty':True}]
        
        if 'Trip' in uplevel['Trips']:
            #if dictionary instead of list - only 1 trip
            if isinstance(uplevel['Trips']['Trip'],dict):
                for key in _key:
                    trip[key] = uplevel['Trips']['Trip'][key]
                return [stopsum,route_dict,trip]
            else:
                for i in range(len(uplevel['Trips']['Trip'])):
                    for key in _key:
                        trip[key] = uplevel['Trips']['Trip'][i][key]
                    _triplist.append([stopsum,route_dict,trip])
                    trip = {}
                return _triplist

        else:

            #if no list
            if isinstance(uplevel['Trips'],dict):
                for key in _key:
                    trip[key] = uplevel['Trips'][key]
                    return [stopsum,route_dict,trip]
                #list of dicts containing specified keys
                else:
                    for i in range(len(uplevel['Trips'])):
                        for key in _key:
                            trip[key] = uplevel['Trips'][i][key]
                        _triplist.append([stopsum,route_dict,trip])
                        trip = {}
                    return _triplist

def extract_values(_json):
    
    stopsum = stop_summary(_json)
    if stopsum is not None:
        trip_list = route_trip(_json, stopsum)
        
    else:
        return None
    
    return trip_list
#testing on one large station (tunneys)
#print(big_list[100])

#print(stop_summary(big_list[100]))
#routes, trips = route(big_list[100])
#print(trips)
    
print(extract_values(r_json))

