#!/bin/bash                                                                                                                                                                             
hadoop jar /usr/hdp/2.6.5.0-292/hadoop-mapreduce/hadoop-streaming.jar -files autoinc_mapper1.py,autoinc_reducer1.py -mapper autoinc_mapper1.py -reducer autoinc_reducer1.py -input /test
_data/input/data.csv -output /test_data/output/all_accidents                                                                                                                            
                                                                                                                                                                                        
hadoop jar /usr/hdp/2.6.5.0-292/hadoop-mapreduce/hadoop-streaming.jar -files autoinc_mapper2.py,autoinc_reducer2.py -mapper autoinc_mapper2.py -reducer autoinc_reducer2.py -input /test
_data/output/all_accidents -output  /test_data/output/make_year_count                                                                                                                   
~                                                                      