#!/usr/bin/python                                                                                                                                                                       
# -*-coding:utf-8 -*
import sys                                                                                                                                                                              
# [Define group level master information]                                                                                                                                               
vehicle_key = None                                                                                                                                                                      
count = 0                                                                                                                                                                               
                                                                                                                                                                                        
# input comes from STDIN                                                                                                                                                                
for line in sys.stdin:                                                                                                                                                                  
# [parse the input we got from mapper and update the master info]                                                                                                                       
    line = line.strip().split(',')                                                                                                                                                      
# [detect key changes]                                                                                                                                                                  
    if vehicle_key != line[0]:                                                                                                                                                          
        if vehicle_key != None:                                                                                                                                                         
# write result to STDOUT                                                                                                                                                                
            print '%s,%s' % (vehicle_key, count)                                                                                                                                        
        count = 0                                                                                                                                                                       
# [update more master info after the key change handling]                                                                                                                               
    vehicle_key = line[0]                                                                                                                                                               
    count += 1                                                                                                                                                                          
                                                                                                                                                                                        
# do not forget to output the last group if needed!                                                                                                                                     
print '%s,%s' % (vehicle_key, count)  