# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os
import argparse
import thread
from db import mysql_
from read_data import send_data


# 1.通过创建输入DStream来定义输入源
# 2.通过对DStream应用转换操作和输出操作来定义流计算
# 3.streamingContext.start() 来开始接收数据和处理流程
# 4.通过streamingContext.awaitTermination()方法来等待处理结束
# 5.可以通过streamingContext.stop()来手动结束流计算进程


def save_data(iter):
    mysql_client = mysql_.DB("root", "passwd", "localhost", "3306", "netflow")
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
        # conf = SparkConf()
        # conf.setMaster("spark://localhost:7077")
        # conf.setAppName("netflow application")
        # sc = SparkContext(conf=conf)
        # self.ssc = StreamingContext(sc, inter_time)
        # self.ssc.checkpoint(dire)
        # self.lines = self.ssc.socketTextStream("localhost", port)
        self.sc = SparkContext(master="local[2]", appName="aggregation traffic")
        self.ssc = StreamingContext(self.sc, inter_time)
        self.ssc.checkpoint(dire)
        self.lines = self.ssc.socketTextStream("localhost", port)

    @staticmethod
    def data_to_30s(n_data):
        date_, time_, ip, traffic = n_data.split(" ")
        h, m, s = time_.split(":")
        s = 0 if s < 30 else 30
        time_ = "{}:{}:{}".format(h, m, s)
        return "{} {} {}".format(date_, time_, ip), traffic

    def seconds_handle_from_iface(self):
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


if __name__ == "__main__":
    if os.name == 'nt':
        default_pidfile = r'%TEMP%\netflow.pid'
    elif os.name == 'posix':
        default_pidfile = r'/tmp/netflow.pid'
    else:
        default_pidfile = None

    ap = argparse.ArgumentParser(description="read net card data to a MySQL database.")
    ap.add_argument('--port', '-p', default="50003", type=int, help="emit netflow handled data to tcp port")

    args = ap.parse_args()

    if args.port < 30001 or args.port > 65535:
        ap.exit(-1, "error: port must be 30001-65535")

    thread.start_new(send_data, (args.port,))

    spark = Spark(1, "/home/checkpoint", args.port)
    spark.seconds_handle_from_iface()
