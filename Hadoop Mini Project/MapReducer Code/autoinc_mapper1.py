#!/usr/bin/python                                                                                                                                                                       
# -*-coding:utf-8 -*                                                                                                                                                                    
import sys                                                                                                                                                                              
vehicle_list = []                                                                                                                                                                       

#method to sort list by vin number                                                                                                                                                                                      
def vin_sort(data):                                                                                                                                                                     
    return data[0]                                                                                                                                                                      
                                                                                                                                                                                        
# input comes from STDIN (standard input)                                                                                                                                               
for line in sys.stdin:                                                                                                                                                                  
# [derive mapper output key values]                                                                                                                                                     
    line = line.strip().split(',')                                                                                                                                                      
    vehicle_list.append([line[2],line[1],line[3],line[5]])                                                                                                                              
                                                                                                                                                                                        
#sort list by vin number  
vehicle_list.sort(key = vin_sort)                                                                                                                                                       
                                                                                                                                                                                        
for i in vehicle_list:                                                                                                                                                                  
    print '%s,%s,%s,%s' % (i[0],i[1],i[2],i[3])  