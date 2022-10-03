#!/usr/bin/env python3.6
import sys
vehicle_dict = {}
vehicle_list = []
# input comes from STDIN (standard input)
for line in sys.stdin:
# [derive mapper output key values]
    line = line.strip().split(',')
    #If initial sales incident type, store make and year for vin number in a dictionary
    if line[1] == 'I':
        vehicle_dict[line[2]] = {"incident_type": line[1],"make":line[3],"year":line[5]}
    # append mapper key values to list
        vehicle_list.append([line[2],line[1],line[3],line[5]])
    else: 
    # For accident and repair incident type, fetch make and year from dictionary and append to list
        make = vehicle_dict[line[2]]["make"]
        year = vehicle_dict[line[2]]["year"]
        vehicle_list.append([line[2],line[1],make,year])
for i in vehicle_list:
    print(f"{i[0]},{i[1]},{i[2]},{i[3]}")
