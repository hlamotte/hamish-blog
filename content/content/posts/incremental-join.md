---
title: "Incremental join using AWS Glue Bookmarks"
date: 2020-10-17T10:42:12+13:00
draft: false
comments: true
tags: ['data engineering', 'aws']
author: "Hamish Lamotte"
showToc: true
hidemeta: false
#cover:
  #image: '/images/hands-join.jpg'
  # can also paste direct link from external site
  # ex. https://i.ibb.co/K0HVPBd/paper-mod-profilemode.png
  #alt: '<alt text>'
  #caption: 'ABC'
---

## The problem
I was recently presented the challenge to join two timeseries datasets together on their timestamps without requiring the corresponding data from either dataset to arrive at the same time as the other. For example, data from one day last month from one dataset may have landed on S3 a week ago, and the corresponding data from the other dataset for that day last month may have landed yesterday. This is an incremental join problem.

## Potential solutions
- It may be possible to get around this problem by holding off from joining the data until it was queried, however I wanted to pre-process the join so that the joined data could be queried directly at any scale.
- You could re-process the entire dataset every pipeline run. This was not an for a dataset that is constantly larger every day.
- It would also be possible to manually implement a system that does not process data from either tables until both have landed. This would in-effect be re-implementing a feature that is already available with AWS Glue: [Bookmarks](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html) which we are going to leverage below.

## Using AWS Glue Bookmarks and predicate pushdown
AWS Glue Bookmarks allows you to only process the new data that has landed in a data pipeline since the pipeline was previously run. In the incremental join problem described above, where corresponding data that needs processed may have landed and have been processed in different runs of the pipeline, this does not fully solve the problem as corresponding data will be fed by the bookmarks to be processed in different jobs so would not be joined.

The solution we came up with leveraged another feature of AWS Glue, the ability to load a subset of a table using a [predicate pushdown](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-partitions.html). We ended up with the following ETL steps in the Glue ETL job:

1. Load the new data that has landed since the last pipeline run using Glue Bookmarks from the AWS Glue catalog.
``` python
table1_new = glueContext.create_dynamic_frame.from_catalog(database="db", table_name="table1", transformation_ctx='table1_new')

table2_new = glueContext.create_dynamic_frame.from_catalog(database="db", table_name="table1", transformation_ctx='table2_new')
```

2. Find the partitions that are being affected by in your new data. As this was timeseries data, it made sense to partition by datetime. Write these partitions into a pushdown predicate that can be queried on the entire dataset.
```python
# convert to PySpark dataframes
table1_new_df = table1_new.toDF()
table2_new_df = table2_new.toDF()

# the example below shows building the pushdown predicate for two levels of paritioning, partition1 and partition2 where partition1 > partition2

table1_partition1 = table1_new_df.select("partition1").distinct().collect()
table1_partition1 = [x[0] for x in table1_partition1]

table2_partition1 = table2_new_df.select("partition2").distinct().collect()
table2_partition1 = [x[0] for x in table1_partition1]

# get the partitions in partition 1 touched by both tables
partition1 = list(set(table1_partition1).union(set(table2_partition1)))

table1_predicate = ''
table2_predicate = ''
for p1 in partition1:

    # for every partition in partition1
    # get partitions touched in partition2

    table1_partition2 = table1_new_df.where(table1_new_df['partition1'] == p1).select('partition2').distinct().collect()
    table1_partition2 = [x[0] for x in table1_partition2]

    table2_partition2 = table2_new_df.where(table2_new_df['partition1'] == p1).select('partition2').distinct().collect()
    table2_partition2 = [x[0] for x in table2_partition2]

    partition2 = list(set(table1_partition2).union(set(table2_partition2)))
    
    # write partitions to pushdown predicate string
    table1_query = ''
    table2_query = ''
    for p2 in partition2:
        table1_query += f"(partition1=='{p1}' and partition2=='{p2}') or "
        table2_query += f"(partition1=='{p1}' and partition2=='{p2}') or "
       
    table1_predicate += table1_query
    table2_predicate += table2_query
    
# tidy up predicate strings
table1_predicate = table1_predicate[:-4]
table2_predicate = table2_predicate[:-4]
```

3. Having built the predicate string which lists every partition for which there is new data in either one or both of the tables, we can now load 'unlock' just that data from the source data tables.
```python
# we use the predicate string we previously built and load the table without using bookmarks
table1_unlock = glueContext.create_dynamic_frame.from_catalog(database="db", table_name="table1", push_down_predicate=table1_predicate)

table2_unlock = glueContext.create_dynamic_frame.from_catalog(database="db", table_name="table2", push_down_predicate=table2_predicate)
```

4. We can now run whatever join transformations we want using these two tables.

5. We can then write the tables to a database, in our case S3. Depending on the type of join transformations you are doing, we found it best to use the Spark API writer in "overwrite" mode rather than the Glue DynamicFrame writers as we wanted to delete any old data that was written in a partition in a previous run and write only the newly processed data.
```python
final_df.write.partitionBy(["partition1", "partition2"]).saveAsTable("db.output", format='parquet', mode='overwrite', path='s3://your-s3-path')
```

Note that the PySpark API mode of writing to the Glue Catalog seems to occasionally cause the table to become unavailable when it is being written to.

## Conclusion
Using AWS Glue Bookmarks in combination with predicate pushdown enables incremental joins of data in your ETL pipelines without reprocessing of all data every time. Good choice of a partitioning schema can ensure that your incremental join jobs process close to the minimum amount of data required.