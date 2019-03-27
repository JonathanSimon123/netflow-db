# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os
import argparse
from netflow import netflow_v10 as nv


#
# 1.通过创建输入DStream来定义输入源
# 2.通过对DStream应用转换操作和输出操作来定义流计算
# 3.streamingContext.start() 来开始接收数据和处理流程
# 4.通过streamingContext.awaitTermination()方法来等待处理结束
# 5.可以通过streamingContext.stop()来手动结束流计算进程


class Spark(object):
    def __init__(self, name, inter_time, dire, host, port):
        self.sc = SparkContext(appName=name)
        self.ssc = StreamingContext(self.sc, inter_time)
        self.ssc.checkpoint(dire)
        self.lines = self.ssc.socketTextStream(host, port)

    def seconds_handle(self):
        words = self.lines.map(lambda line: line.split(" "))
        # 每秒聚合
        pairs = words.map(lambda word: ("{} {} {}".format(word[0], word[1], word[2]), int(word[3])))

        # 每秒聚合
        word_counts = pairs.reduceByKey(lambda x, y: x + y)
        word_counts.pprint()

    def seconds_30_handle(self):
        # 每30秒聚合
        pairs_30 = self.lines.map(self.data_to_30s)

        # 每30秒聚合, 过度窗口60s, 滑动窗口40s
        windowed_word_counts = pairs_30.reduceByKeyAndWindow(lambda x, y: x + y, 40, 40)
        windowed_word_counts.foreachRDD()  # 为什么是按batchDuration 来打印

        self.ssc.start()
        self.ssc.awaitTermination()

    @staticmethod
    def data_to_30s(n_data):
        date_, time_, ip, traffic = n_data.split(" ")
        h, m, s = time_.split(":")
        s = 0 if s < 30 else 30
        time_ = "{}:{}:{}".format(h, m, s)
        return "{} {} {}".format(date_, time_, ip), traffic


if __name__ == "__main__":
    if os.name == 'nt':
        default_pidfile = r'%TEMP%\netflow.pid'
    elif os.name == 'posix':
        default_pidfile = r'/tmp/netflow.pid'
    else:
        default_pidfile = None

    ap = argparse.ArgumentParser(description="Copy Netflow data to a MySQL database.")
    ap.add_argument('--daemonize', '-d', action='store_true', help="run in background")
    ap.add_argument('--pidfile', type=str, default=default_pidfile, help="location of pid file")
    ap.add_argument('--dbuser', '-U', default="netflow", help="database user")
    ap.add_argument('--dbpassword', '-P', help="database password")
    ap.add_argument('--dbhost', '-H', default="127.0.0.1", help="database host")
    ap.add_argument('--dbname', '-D', default="netflow", help="database name")
    ap.add_argument('receive_port', type=int, help="Netflow UDP listener port")
    ap.add_argument('tcp_port', type=int, help="emit netflow handled data to tcp port")
    ap.add_argument('--quiet', '-q', action='store_true',
                    help="Suppress console messages (only warnings and errors will be shown")

    args = ap.parse_args()
    if args.port < 1 or args.port > 65535:
        ap.exit(-1, "error: port must be  1-65535")

    et = nv.EmitDataToTcpPort
    # 处理netflow数据然后开启TCP服务器，发生数据
    et.do(args.receive_port, args.tcp_port)

    spark = Spark(sys.argv[0], sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
    # spark.seconds_30_handle()
    spark.seconds_handle()
