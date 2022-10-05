1.  Used <https://www.youtube.com/watch?v=735yx2Eak48> to install
    Virtual Box and Cloudera HDP sandbox environment

    a.  Points to note in this:

        i.  HDP 2.6.5 enabled the required local host port to open the
            shell in Mac OS. 2.5.0 did not work with latest virtual box
            environment for me

        ii. Once in the shell within the virtual box, determine path of
            python and add to the python file at the top
            (\#!/usr/bin/python). Python2 was used for this

        iii. Also determine where Hadoop-streaming.jar is present using
             command:

> find /usr/hdp -name hadoop-streaming.jar

2.  Instructions to run this program

    a.  Login to sandbox Ambari UI environment using admin user id and
        go to Files View (explained in video from point 1 above)

> http://sandbox-hdp.hortonworks.com:8080/\#/login

b.  Create a new directory called test_data under "/" and another
    directory called input within test_data. Place everything under the
    MapReducer code and data folder in the github repository within the input
    directory.

c.  From virtual box shell, go to "/" directory and run following
    commands:

    i.  mkdir documents

    ii. cd /documents

    iii. hadoop fs -get /test_data/input/\*.py

    iv. hadoop fs -get /test_data/input/\*.sh

    v.  ./autoinc_mapreduce.sh

d.  Check output in Ambari UI under /test_data/output directory as shown
    in Output.docx file
