---
title: Incremental window functions using AWS Glue Bookmarks
date: 2020-10-20T19:46:54+13:00
draft: false
comments: true
tags: ['data engineering', 'aws', 'apache spark']
author: "Hamish Lamotte"
showToc: true
hidemeta: false
---
## The out-of-order data landing problem
Applying window functions over data is non-trivial if data arrives out-of-order (with respect to the dimension the window function is applied across). For clarity, lets take timeseries data for this example as our window dimension. If timeseries data arrives from Tuesday through Thursday of a week, then at a later time data from Monday of that week arrives, the data has arrived out-of-order.

As a window function output is sensitive to its surroundings in timespace, the results of the window function would be altered by the new out-of-order data that landed. All affected data needs to be reprocessed.

You could reprocess all the data when data arrives out-of-order. But, when data quantities are large, reprocessing the entire dataset becomes impractical. This article discusses an efficient approach, using the approach building an AWS Glue predicate pushdown described in my previous [article](incremental-join.md). This approach only reprocesses the data affected by the out-of-order data that has landed.

## Solution

### Glue ETL Job environment setup
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
```

### Inspect new data
Use AWS Glue Bookmarks to feed only new data into the Glue ETL job.

```python
new = glueContext.create_dynamic_frame.from_catalog(database="db", table_name="table", transformation_ctx='new')
```

Find the earliest timestamp partition for each partition that is touched by the new data.

Note: in this example below the data is partitioned as `partition1` > `timestamp_partition` where `timestamp_partition` is the only timeseries partitioning.

```python
# convert to Spark dataframe
new_df = new.toDF()

# get minimum timestamp for each other partition
min_timestamps = new_df.groupby(new_df.partition1).min("timestamp_partition")

# convert to local iterator
min_timestamps = min_timestamps.rdd.map(tuple).toLocalIterator()
```

Build the pushdown predicate string of the data from entire dataset that needs to be processed/re-processed. For partitions without data that had landed out-of-order, manually define a window of data to unlock into the past to ensure new data landing in order is processed correctly.

Note: in the example below we are partitioning in `timestamp_partition` by date.

```python
# number of days to unlock in the past (number of days since last data processing run in order to catch old data)
unlock_window = 3

import datetime

today = int(datetime.datetime.now().strftime('%Y%m%d'))

date_search = today - past_days

date_predicate = ''
for p1, earliest_date in min_timestamps:
# choose the minimum of the dates touched by new data and the window of minimum data from the past we want to unlock
    date = min(date_search, earliest_date)
    predicate_string = f"(partition1=='{p1}' and timestamp_partition>='{earliest_date}') or "
       
    date_predicate += predicate_string
    
# tidy predicate
date_predicate = date_predicate[:-4]
```
### Applying the window function on all required data
Now we can load all the data that needs to be processed/re-processed using the predicate string we just built.
```python
# query all data that needs to be updated
processed = glueContext.create_dynamic_frame.from_catalog(database="db", table_name="table", push_down_predicate=date_predicate)
```

Then we can define our window on all the data loaded.

```python
from pyspark.sql import Window

window = Window.partitionBy("partition1").orderBy("timestamp_partition").rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

Apply your function on your windows, we use the `last` function as an example here.

```python
processed = processed.select(*((last(c, ignorenulls=True).over(window)).alias(c) for c in processed.columns))
```

To ensure that any old data that has been changed during its reprocessing is overwritten use the PySpark API `overwrite` mode directly to S3.

```python
# set the overwrite mode to dynamic
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# write to s3 in overwrite mode
processed.write.partitionBy(["partition1", "timestamp_partition"]).saveAsTable("db.table", format='parquet', mode='overwrite', path='s3://your-s3-path/')
```

## Conclusion
Building pushdown predicate strings from the new data delivered from the AWS Glue Bookmarks enables only processing/re-processing the required partitions of dataset, even when using window functions, which are inherently sensitive to their surroundings within the dataset.