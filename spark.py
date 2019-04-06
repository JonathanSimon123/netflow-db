# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os
import argparse
from netflow import netflow_v10 as nv
import thread
from db import mysql_


# 1.通过创建输入DStream来定义输入源
# 2.通过对DStream应用转换操作和输出操作来定义流计算
# 3.streamingContext.start() 来开始接收数据和处理流程
# 4.通过streamingContext.awaitTermination()方法来等待处理结束
# 5.可以通过streamingContext.stop()来手动结束流计算进程


def save_data(iter):
    mysql_client = mysql_.DB("root", "passwrod", "localhost", "3366", "netflow")
    mysql_client.connect()
    for record in iter:
        date_, time_, ip = record[0].split(" ")
        sql = "insert into netflow  (ip, seconds_sum, time,date)values('%s', %d, '%s', '%s')" % (
            ip, record[1], time_, date_)
        print(sql)
        try:
            mysql_client.insert(sql)
        except Exception as e:
            raise e


class Spark(object):
    def __init__(self, inter_time, dire, port):
        conf = SparkConf()
        conf.setMaster("spark://localhost:7077")
        conf.setAppName("netflow application")
        sc = SparkContext(conf=conf)
        self.ssc = StreamingContext(sc, inter_time)
        self.ssc.checkpoint(dire)
        self.lines = self.ssc.socketTextStream("localhost", port)

    def seconds_handle(self):
        words = self.lines.map(lambda line: line.split(" "))
        # 每秒聚合
        pairs = words.map(lambda word: ("{} {} {}".format(word[0], word[1], word[2]), int(word[3])))
        # 每秒聚合, 过度窗口2s,滑动窗口1
        windowed_word_counts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, 1, 1)

        # 数据存入mysql
        windowed_word_counts.foreachRDD(
            lambda rdd: rdd.foreachPartition(
                lambda iter: save_data(iter)
            )
        )

        self.ssc.start()
        self.ssc.awaitTermination()

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
    ap.add_argument('--receive_port', default="30001", help="Netflow UDP listener port")
    ap.add_argument('--port', '-p', default="50003", help="emit netflow handled data to tcp port")

    args = ap.parse_args()

    if args.receive_port < 1 or args.receive_port > 65535:
        ap.exit(-1, "error: port must be  1-65535")

    et = nv.EmitDataToTcpPort
    # 处理netflow数据然后开启TCP服务器，发生数据
    thread.start_new_thread(et.do, (args.receive_port, args.tcp_port))

    spark = Spark(1, "/home/checkpoint", args.tcp_port)
    spark.seconds_handle()
