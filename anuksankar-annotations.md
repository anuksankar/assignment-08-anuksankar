## W205 - Assignment 8 - Build and Write-up Pipeline

### Spark Stack with Kafka and HDFS

#### Setup - As we have already created a directory ~/w205/spark-with-kafka-hdfs in class, just cd into it for the assignment.
```
cd ~/w205/spark-with-kafka-and-hdfs

cp ~/w205/course-content/08-Querying-Data/docker-compose.yml .

```

Spin up the cluster
```
docker-compose up -d

docker-compose logs -f kafka
```
Check out Hadoop

```
docker-compose exec cloudera hadoop fs -ls /tmp/
```
The following was displayed:
```
Found 2 items
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2018-11-01 23:24 /tmp/hive
```

Create topic "assessments"

```
docker-compose exec kafka kafka-topics --create --topic assessments --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
```
Displays
```
Created topic "assessments".
```
As I am using the same json file from assignments 6 and 7, I just  verified if it is there in w205 directory.
```
cd ~/w205

ls -l 
```
lists assessment-attempts-20180128-121051-nested.json

Change directory to:
```
cd ~/w205/spark-with-kafka-and-hdfs
```

Use kafkacat to produce messages to the assessments topic
```
docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t assessments"
```

Spin up a pyspark process using the spark container
```
docker-compose exec spark pyspark
```

At the pyspark prompt, read from kafka
```
raw_assessments = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe","assessments").option("startingOffsets", "earliest").option("endingOffsets", "latest").load() 
```
Cache this
```
raw_assessments.cache()
```
displays 
```
DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
```
See what I have
```
raw_assessments.printSchema()
```
Displays
```
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
```

Cast as strings
```
assessments = raw_assessments.selectExpr("CAST(value AS STRING)")
```
Write that to hdfs
```
assessments.write.parquet("/tmp/assessments")
```

From another terminal window, checkout the results
```
docker-compose exec cloudera hadoop fs -ls /tmp/
```
Displays
```
Found 3 items
drwxr-xr-x   - root   supergroup          0 2018-11-07 21:01 /tmp/assessments
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2018-11-01 23:24 /tmp/hive
```

```
docker-compose exec cloudera hadoop fs -ls /tmp/assessments/
```
Displays
```
Found 2 items
-rw-r--r--   1 root supergroup          0 2018-11-07 21:01 /tmp/assessments/_SUCCESS
-rw-r--r--   1 root supergroup    2513397 2018-11-07 21:01 /tmp/assessments/part-00000-9b8c0712-e0fc-4a85-85ec-7dd94c02f18f-c000.snappy.parquet
```

Switch back to the spark terminal window and what we wrote
```
assessments.show()
```
Displays
```
+--------------------+
|               value|
+--------------------+
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|....................|
|{"keen_timestamp"...|
+--------------------+
only showing top 20 rows

```

Publish some more stuff to kakfa
```
docker-compose exec mids bash -c "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t commits"
```
Read At PySpark terminal
```
raw_assessments = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe","commits").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
```
Cache this to cut back on warnings
```
raw_assessments.cache()
```
Displays
```
DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
```
Cast as string
```
assessments = raw_assessments.select(raw_assessments.value.cast('string'))
```

Extract Data, Deal with unicode
```
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
```
Take a look at the file
```
import json
from pyspark.sql import Row
extracted_assessments = assessments.rdd.map(lambda x: Row(**json.loads(x.value))).toDF()
extracted_assessments.registerTempTable('assessments')
spark.sql("select keen_id from assessments limit 10").show()
```
Displays
```
+--------------------+
|             keen_id|
+--------------------+
|5a6745820eb8ab000...|
|5a674541ab6b0a000...|
|5a67999d3ed3e3000...|
|5a6799694fc7c7000...|
|5a6791e824fccd000...|
|5a67a0b6852c2a000...|
|5a67b627cc80e6000...|
|5a67ac8cb0a5f4000...|
|5a67a9ba060087000...|
|5a67ac54411aed000...|
+--------------------+
```

sequence.questions is a list
```

spark.sql("select keen_timestamp, sequences.questions[0].user_incomplete from assessments limit 10").show()
+------------------+-------------------------------------------------------+
|    keen_timestamp|sequences[questions] AS `questions`[0][user_incomplete]|
+------------------+-------------------------------------------------------+
| 1516717442.735266|                                                   true|
| 1516717377.639827|                                                  false|
| 1516738973.653394|                                                  false|
|1516738921.1137421|                                                  false|
| 1516737000.212122|                                                  false|
| 1516740790.309757|                                                  false|
|1516746279.3801291|                                                  false|
| 1516743820.305464|                                                  false|
|  1516743098.56811|                                                  false|
| 1516743764.813107|                                                  false|
+------------------+-------------------------------------------------------+
```

Spark infers null for missing values in json objects.  "abc123" is a made-up column.
```
spark.sql("select sequences.abc123 from assessments limit 10").show()
```
Displays
```
+------+
|abc123|
+------+
|  null|
|  null|
|  null|
|  null|
|  null|
|  null|
|  null|
|  null|
|  null|
|  null|
+------+
```
As sequences is a dictionary of dictionaries,  the following does not work. We get an exception.  
``` 

spark.sql("select sequence.id from assessments limit 10").show()
```
Extracting using custom lambda function
```
def my_lambda_sequences_id(x):
    raw_dict = json.loads(x.value)
    my_dict = {"keen_id" : raw_dict["keen_id"], "sequences_id" : raw_dict["sequences"]["id"]}
    return my_dict

my_sequences = assessments.rdd.map(my_lambda_sequences_id).toDF()
```
Displays
```
/spark-2.2.0-bin-hadoop2.6/python/pyspark/sql/session.py:351: UserWarning: Using RDD of dict to inferSchema is deprecated. Use pyspark.sql.Row instead
  warnings.warn("Using RDD of dict to inferSchema is deprecated. "
```
More of the lambda function...
```
my_sequences = assessments.rdd.map(my_lambda_sequences_id).toDF()
my_sequences.registerTempTable('sequences')
spark.sql("select sequences_id from sequences limit 10").show()
```
displays the sequences_id
```
+--------------------+
|        sequences_id|
+--------------------+
|5b28a462-7a3b-42e...|
|5b28a462-7a3b-42e...|
|b370a3aa-bf9e-4c1...|
|b370a3aa-bf9e-4c1...|
|04a192c1-4f5c-4ac...|
|e7110aed-0d08-4cb...|
|5251db24-2a6e-424...|
|066b5326-e547-4da...|
|8ac691f8-8c1a-403...|
|066b5326-e547-4da...|
+--------------------+
```
```
spark.sql("select a.keen_id, a.keen_timestamp, s.sequences_id from assessments a join sequences s on a.keen_id = s.keen_id limit 10").show()
```
displays
```
+--------------------+------------------+--------------------+
|             keen_id|    keen_timestamp|        sequences_id|
+--------------------+------------------+--------------------+
|5a17a67efa1257000...|1511499390.3836269|8ac691f8-8c1a-403...|
|5a26ee9cbf5ce1000...|1512500892.4166169|9bd87823-4508-4e0...|
|5a29dcac74b662000...|1512692908.8423469|e7110aed-0d08-4cb...|
|5a2fdab0eabeda000...|1513085616.2275269|cd800e92-afc3-447...|
|5a30105020e9d4000...|1513099344.8624721|8ac691f8-8c1a-403...|
|5a3a6fc3f0a100000...| 1513779139.354213|e7110aed-0d08-4cb...|
|5a4e17fe08a892000...|1515067390.1336551|9abd5b51-6bd8-11e...|
|5a4f3c69cc6444000...| 1515142249.858722|083844c5-772f-48d...|
|5a51b21bd0480b000...| 1515303451.773272|e7110aed-0d08-4cb...|
|5a575a85329e1a000...| 1515674245.348099|25ca21fe-4dbb-446...|
+--------------------+------------------+--------------------+
``` 
Save some info
```

some_assessments_info = spark.sql("select a.keen_id, a.keen_timestamp, s.sequences_id from assessments a join sequences s on a.keen_id = s.keen_id limit 10")
```

Write it out to hdfs
```
some_assessments_info.write.parquet("/tmp/some_assessments_info")
```

Check out the results in the hadoop terminal window
```
docker-compose exec cloudera hadoop fs -ls /tmp/
```
Displays
```
Found 4 items
drwxr-xr-x   - root   supergroup          0 2018-11-07 21:01 /tmp/assessments
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2018-11-01 23:24 /tmp/hive
drwxr-xr-x   - root   supergroup          0 2018-11-07 23:04 /tmp/some_assessments_info
```
and 

```
docker-compose exec cloudera hadoop fs -ls /tmp/some_assessments_info
```
Displays
```
Found 2 items
-rw-r--r--   1 root supergroup          0 2018-11-07 23:04 /tmp/some_assessments_info/_SUCCESS
-rw-r--r--   1 root supergroup       1768 2018-11-07 23:04 /tmp/some_assessments_info/part-00000-d48b0922-335b-494a-8eec-cc5ce8233436-c000.snappy.parquet
```

Exit from hapood and pyspark
```
exit()

exit

```
Bring down the cluster
```
docker-compose down
```


