"""
Quick and dirty script to write a json out to a text file
"""
import datetime
import time

def write_to_text(fpath,_json):
    
	date = datetime.datetime.today().strftime('%Y-%m-%d')
	name = "{}\ocjson-{}-{}.txt".format(fpath,date,int(time.time()))
	
	f= open(name,"w+")
	f.write(_json)
	f.close()
	
	return
	
