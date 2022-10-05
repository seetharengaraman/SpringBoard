#!/usr/bin/python                                                                                                                                                                       
# -*-coding:utf-8 -*                                                                                                                                                                    
import sys                                                                                                                                                                              
                                                                                                                                                                                        
# [Define group level master information]                                                                                                                                               
vehicle_list = []                                                                                                                                                                       
accident_list = []                                                                                                                                                                      
# input comes from STDIN                                                                                                                                                                
for line in sys.stdin:                                                                                                                                                                  
                                                                                                                                                                                        
# [parse the input we got from mapper and update the master info]                                                                                                                       
    line = line.strip().split(',')                                                                                                                                                      
    if line[1] == 'A':                                                                                                                                                                  
        accident_list.append(line[0])                                                                                                                                                   
    elif line[1] =='I':                                                                                                                                                                 
        vehicle_list.append([line[0],line[2],line[3]])                                                                                                                                                                                                                                                                        

#filter vehicle list by vehicles that had accidents                                                                                                                                                                                        
for i in accident_list:                                                                                                                                                                 
    for j in vehicle_list:                                                                                                                                                              
#        print '%s-%s' % (i, j[0])                                                                                                                                                      
        if i == j[0]:                                                                                                                                                                   
            print '%s,%s,%s' % (j[0],j[1],j[2])                                                                                                                                         
            break  