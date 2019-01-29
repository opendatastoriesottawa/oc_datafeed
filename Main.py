# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 19:41:44 2018

@author: douga
"""

import OC_API_Functions as ocf
import schedule

import config as cfg



#note - times are -2 minutes from actual schedule time
#list_times95e_3047 = ['9:53','10:23','10:54']
#list_times95e_3005 = ['10:28','10:58','11:29']
#list_times95e_3027 = ['11:00','11:30','12:01']
#list_stops95e = [[3047,list_times95e_3047], [3005,list_times95e_3005], [3027,list_times95e_3027]]
#list_routes = [[95,list_stops95e,]]

#for testing, create a global list which will take in all the data
big_list = []
counter = 0

def Main():
    
    schedule.clear()
    ocf.sched_oc_call(big_list, counter, cfg.app_id, cfg.app_key, cfg.acc_url, cfg.fpath)
    
   
Main()


"""
Program Specs

Should: take in a list of times, routes etc.
Calculate the first time, run the scheduler on this
The job run is always the same - make API call to OCT, then save returning value to DB




"""


#import gc
#import sys
#
#size = 0
#for i in range(len(big_list)):
#    size += sys.getsizeof(big_list[i])
#print(size)    
#
#
#def get_obj_size(obj):
#    marked = {id(obj)}
#    obj_q = [obj]
#    sz = 0
#
#    while obj_q:
#        sz += sum(map(sys.getsizeof, obj_q))
#
#        # Lookup all the object reffered to by the object in obj_q.
#        # See: https://docs.python.org/3.7/library/gc.html#gc.get_referents
#        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))
#
#        # Filter object that are already marked.
#        # Using dict notation will prevent repeated objects.
#        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}
#
#        # The new obj_q will be the ones that were not marked,
#        # and we will update marked with their ids so we will
#        # not traverse them again.
#        obj_q = new_refr.values()
#        marked.update(new_refr.keys())
#
#    return sz
#
#get_obj_size(big_list)