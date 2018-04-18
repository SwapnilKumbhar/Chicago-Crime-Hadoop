#!/usr/bin/env python
import json
import os
import time

from logger import *
from HouseKeeping import *
from HDriver import *
from Plotter import *

config = open('config.json','r').read()
d_config = json.loads(config)

hdb = HDriver(d_config)
housekeeping = HouseKeeping(d_config)
plotter = Plotter(d_config)

dlog("This is a driver program for Hadoop. [Dataset: Chicago Crime from 2001 to 18th March 2018]")
method = raw_input("What method [all, year, limit]? ")
dlog(method)
if method == 'all':
    hdb.handle(method)
elif method == 'limit':
    l_lim = raw_input("Enter Lower Limit [mm/yyyy]: ")
    u_lim = raw_input("Enter Upper Limit [mm/yyyy]: ")
    hdb.handle(method, lower=l_lim, upper=u_lim)
elif method == 'year':
    year = raw_input("Enter year [yyyy]: ")
    hdb.handle(method, year=year)
else:
    dlog("No Method Found. Exiting...")
    exit()

dlog("Getting data to local")
housekeeping.get()

dlog("Building Plot")
plotter.plot()

dlog("Showing Gathered data.")
housekeeping.show()

dlog("Press any key to continue")
raw_input()

dlog("Cleaning up...")
housekeeping.clean()

dlog("Program Exiting...")