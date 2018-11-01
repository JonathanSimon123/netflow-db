# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#
# 1.通过创建输入DStream来定义输入源
# 2.通过对DStream应用转换操作和输出操作来定义流计算
# 3.streamingContext.start() 来开始接收数据和处理流程
# 4.通过streamingContext.awaitTermination()方法来等待处理结束
# 5.可以通过streamingContext.stop()来手动结束流计算进程


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: netflow count <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="netflow count")
    ssc = StreamingContext(sc, 10)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(":"))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)

    counts.print()

    ssc.start()
    ssc.awaitTermination()
