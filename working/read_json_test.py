# -*- coding: utf-8 -*-
"""
Created on Sat Feb  9 16:06:07 2019

@author: douga
"""

import read_json as rj
import os

folder = r'D:\Dougall\Python_Workspace\OCTranspo\outputs'
filelist = os.listdir(folder)


for file in filelist:
    file = "{}\{}".format(folder, file)
    data = rj.__load_json_fromtxt(file)