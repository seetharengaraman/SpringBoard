This is an mini project for using airflow. It deals with downloading Apple and Tesla stock price for a day in 1 minute intervals, moving it to a data location and then querying for the spread across the day (between high and low)
Execution method:
1. Clone springboard-airflow directory into local
2. cd springboard-airflow
3. run ./start.sh to start the airflow services with the data_pipeline.py under ./mnt/airflow/dags directory
4. Go to localhost:8080 to access web ui
5. Use username/pwd as airflow/airflow to login
6. Dag maybe in paused status. Toggle to make it active and then click on run to see it successful. Output for each task should be similar to what is under ./Output/Output.txt file
7. After completing use ./stop.sh to stop the services
8. Use ./reset.sh to completely wipe out all the images.
