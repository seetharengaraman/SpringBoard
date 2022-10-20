Spark Optimization

Original Code -- unoptimized.py

Optimized Code -- optimized.py

Issues noted and optimization done:

1.  There was a shuffle exchange in between grouping and aggregation for
    fetching count of answers by question id and month. Removed that by
    repartitioning upfront.

answersDF.repartition(\'question_id\')

2.  Lower row count data frame (questionsDF) was on the left side of the
    join with answers_month on the right side. Reversed the same

resultDF = answers_month.join(broadcast(questionsDF),
\'question_id\').select(\'question_id\', \'creation_date\', \'title\',
\'month\', \'cnt\').cache()

3.  Data was fetched from disk and hence cached for usage and then
    un-persisted the same.

answers_month =
answersDF.repartition(\'question_id\').withColumn(\'month\',
month(\'creation_date\')).groupBy(\'question_id\',
\'month\').agg(count(\'\*\').alias(\'cnt\')).cache()

4.  For a data set of 110714 records, 200 partitions seemed a lot as
    seen below.

![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/200%20partitions.png)

Set shuffle partitions to 30

spark.conf.set(\"spark.sql.shuffle.partitions\",30)

![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/30%20partitions.png)

Unoptimized query -- First part of scanning answer records taking 14.1
sec (combination of tasks) with another 3.5 sec after shuffle during
grouping/aggregation.

![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/unoptimized%20query%20performance.png)

> With repartition, scanning answer records from file takes 3.7 sec and
> grouping/aggregation takes 11.3 sec
>
> ![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/Repartitioned%20query%20performance.png)
>
> With ensuring answers_month dataframe on the left side of the join,
> its file scan reduced to 2.4 sec followed by grouping and aggregation
> reducing to 8.1 sec (though questionsDF scanning increased to 2.4 sec
> from 1.1 sec, overall reduction in time seen)
>
> ![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/left%20join%20changed%20query%20performance.png)
>
> Now caching helped reduce the total time to a great extent (9s to 6s)
>
> ![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/cache%20performance_1.png)
>
> ![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/cache%20performance_2.png)
>
> Further reducing shuffle partition from 200 to 30 reduced to 4s from
> 6s
>
> ![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/shuffle%20partition%20reduced%20performance_1.png)
>
> ![alt text](https://github.com/seetharengaraman/SpringBoard/blob/main/Spark%20Optimization%20MiniProject/Images/shuffle%20partition%20reduced%20performance_2.png)
