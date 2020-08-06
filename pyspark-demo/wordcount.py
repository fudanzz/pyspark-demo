import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    print(len(sys.argv))

    if (len(sys.argv) != 3):
        print("Usage: spark-etl [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession.builder.appName("SparkETL").getOrCreate()

    book = spark.read.text(sys.argv[1])

    results = (book.select(split(col("value"), " ").alias("line"))
    .select(explode(col("line")).alias("word"))
    .select(lower(col("word")).alias("word"))
    .select(regexp_extract(col("word"), "[a-z']*", 0).alias("word"))
    .where(col("word") != "")
    .where(length(col("word")) > 4)
    .groupby(col("word")).count()
    .orderBy("count", ascending=False))

    results.write.csv(sys.argv[2])

