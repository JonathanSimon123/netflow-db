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


def aad(a, b):
    return a + b


def data_to_30s(n_data):
    date_, time_, ip, traffic = n_data.split(" ")
    h, m, s = time_.split(":")
    s = 0 if s < 30 else 30
    time_ = "{}:{}:{}".format(h, m, s)
    return "{} {} {}".format(date_, time_, ip), traffic


if __name__ == "__main__":
    sc = SparkContext(appName="netflow count")

    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("/home/checkpoint")

    if len(sys.argv) != 3:
        print("Usage: netflow count <hostname> <port>", file=sys.stderr)
        exit(-1)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    # words = lines.map(lambda line: line.split(" "))

    # 每秒聚合
    # pairs = words.map(lambda word: ("{} {} {}".format(word[0], word[1], word[2]), int(word[3])))

    # 每秒聚合
    # wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    # wordCounts.pprint()

    # 每30秒聚合
    pairs_30 = lines.map(data_to_30s)

    # 每1分钟聚合, 过度窗口40s, 滑动窗口30s
    windowedWordCounts = pairs_30.reduceByKeyAndWindow(lambda x, y: x + y, 40, 30)
    windowedWordCounts.pprint()

    ssc.start()
    ssc.awaitTermination()

