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

![](media/image1.png){width="7.5in" height="1.2583333333333333in"}

Set shuffle partitions to 30

spark.conf.set(\"spark.sql.shuffle.partitions\",30)

> ![](media/image2.png){width="7.5in" height="1.2590277777777779in"}

Unoptimized query -- First part of scanning answer records taking 14.1
sec (combination of tasks) with another 3.5 sec after shuffle during
grouping/aggregation.

> ![](media/image3.png){width="3.4071423884514433in"
> height="7.554587707786527in"}
>
> With repartition, scanning answer records from file takes 3.7 sec and
> grouping/aggregation takes 11.3 sec
>
> ![](media/image4.png){width="3.859722222222222in" height="9.0in"}
>
> With ensuring answers_month dataframe on the left side of the join,
> its file scan reduced to 2.4 sec followed by grouping and aggregation
> reducing to 8.1 sec (though questionsDF scanning increased to 2.4 sec
> from 1.1 sec, overall reduction in time seen)
>
> ![](media/image5.png){width="3.7680555555555557in" height="9.0in"}
>
> Now caching helped reduce the total time to a great extent (9s to 6s)
>
> ![](media/image6.png){width="3.3055555555555554in"
> height="3.1944444444444446in"}
>
> ![](media/image7.png){width="7.5in" height="1.6097222222222223in"}
>
> Further reducing shuffle partition from 200 to 30 reduced to 4s from
> 6s
>
> ![](media/image8.png){width="2.64915791776028in"
> height="2.7039676290463692in"}
>
> ![](media/image9.png){width="7.5in" height="1.6159722222222221in"}
