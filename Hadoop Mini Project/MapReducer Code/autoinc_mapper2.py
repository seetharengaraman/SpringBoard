#!/usr/bin/python                                                                                                                              # -*-coding:utf-8 -*                     
import sys                                                                                                                                                                              
                                                                                                                                                                                        
# input comes from STDIN (standard input)                                                                                                                                               
for line in sys.stdin:                                                                                                                                                                  
    line = line.strip().split(',')                                                                                                                                                      
# print composite key of make and year along with accident count of 1                                                                                                                   
    print '%s-%s,%d' % (line[1], line[2],1)  