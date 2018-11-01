from __future__ import print_function

from pyspark.sql import SparkSession

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: wordcount <file>", file=sys.stderr)
    #     sys.exit(-1)

    spark = SparkSession.builder \
            .master("local") \
            .appName("Word Count") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    def p(x):
        print(x)

    result = spark.sparkContext.\
        parallelize([1, 2, 3, 4, 5, 6]).\
        repartition(2).\
        map(lambda x: x * x).\
        filter(lambda x: x >= 10)

    for i in result.take(10):
        print(i)

