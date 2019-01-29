# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 21:39:57 2018

Create a schedule for the program to make API calls
#maximum of 10,000 connections per day

@author: douga
"""

import schedule
import time


def sched_oc_call(my_job, runtimes):
    
    for i in range(runtimes):
        schedule.every(0).seconds.do(my_job)
    
    while True:
        schedule.run_pending()
        schedule.clear()
        
        
    return

def calculate_sleep_time(_time1, _time2):
    
    _time = _time1 - _time2
    _time.seconds
    
    return _time



    
    
