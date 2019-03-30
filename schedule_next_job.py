# -*- coding: utf-8 -*-
"""
Created on Thu Dec 27 13:42:15 2018
 
@author: percido
"""
 
import time
from random import randint

timestamp_fmt = "%H:%M:%S"
 
 
 
def time_period(_time):
  
    #offpeak - 00:00-6:30,19:30-23:59
    #midpeak - 6:30-7:30,9:30-15:30,17:30-19:30
    #peak    - 7:30-9:30, 15:30 - 17:30
    #in seconds
    peak_periods = [[27000,34199],[55800,62999]]
    midpeak_periods = [[23400,26999],[34200,55799],[63000,70200]]
    
    seconds = _time
    for elem in peak_periods:
        if seconds > elem[0] and seconds < elem[1]:
            return "peak"
 
    for elem in midpeak_periods:
        if seconds > elem[0] and seconds < elem[1]:
            return "midpeak"
 
    return "offpeak"
 
#reset counter to 0 at midnight
def reset_counter():
     return 0
 
#if 10000 calls were made on the day, sleep the function until reset at 00:00
def sleep_until_midnight(_seconds, counter):
    final = 86399 #seconds from midnight to 11:59:59
    _sleep = (final - _seconds) + 1
    time.sleep(_sleep)
    return
 
#how many seconds until next call - quicker during peak, slower as we move from peaks. If counter hits max, sleep until midnight, reset
def next_sleep(counter, _time):     
 
    #number of seconds between calls
    #period_secs = {'peak':4, 'midpeak':10, 'offpeak':20}
    #offpeak, midpeak, onpeak - min gap, max gap (seconds)
    _distrib = [[10,35],[5,15],[1,8]]
 
    if time_period(_time) == "offpeak":
        return randint(_distrib[0][0], _distrib[0][1])
    elif time_period(_time) == "midpeak":
        return randint(_distrib[1][0], _distrib[1][1])
    else:
        return randint(_distrib[2][0], _distrib[2][1])
 
 
def increase_count(counter):
    return counter + 1
 