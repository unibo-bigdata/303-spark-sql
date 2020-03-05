# 303 SparkSQL

Module 1, Big Data course (81932), University of Bologna.

## 303-1 Create DataFrames

Import data from JSON file to DataFrame

```shell
# Spark 1
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val df_movies = sqlContext.jsonFile("/bigdata/dataset/movies/movies.json")
# Spark 2
val df_movies = spark.read.json("/bigdata/dataset/movies/movies.json")
val df_movies = spark.read.format("json").load("/bigdata/dataset/movies/movies.json")
```

Load to df a CSV without a defined schema

```shell
val population = sc.textFile("/bigdata/dataset/population/zipcode_population.csv")
val schemaString = "zipcode total_population avg_age male female"
val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
val rowRDD = population.map(_.split(";")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))
val peopleDF = sqlContext.createDataFrame(rowRDD, schema)
```

Load to df a TXT with the schema in the first row

```shell
# Spark 1
val transaction_RDD = sc.textFile("/bigdata/dataset/real_estate/real_estate_transactions.txt")
val schema_array = transaction_RDD.take(1)
val schema_string = schema_array(0)
val schema = StructType(schema_string.split(';').map(fieldName ⇒ StructField(fieldName, StringType, true)))
val rowRDD = transaction_RDD.map(_.split(";")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))
val transaction_DF_tmp = sqlContext.createDataFrame(rowRDD, schema)
//then remove the first row (which was the schema) 
val transaction_DF = transaction_DF_tmp.where("street <> 'street'")
# Spark 2
val df = sqlContext.read.format("csv").option("header", "true").option("delimiter",";").load("/bigdata/dataset/real_estate/real_estate_transactions.txt")
```

Load user data from parquet file

```shell
val parquet_df = sqlContext.read.load("/bigdata/dataset/userdata/userdata.parquet")
```
