# -*- coding: utf-8 -*-
import socket
import time
from scapy.all import *
import argparse


def handler(pdata, connection):
    data = "%s %s %d" % (timestamp_to_str(), pdata[IP].dst, len(pdata[Raw].load))
    print("data: ", data)
    connection.sendall(data + "\r\n")


# 将时间戳转换为日期字符串
def timestamp_to_str(format_='%Y-%m-%d %H:%M:%S', timestamp_=False):
    if timestamp_:
        return time.strftime(format_, time.localtime(timestamp_))
    return time.strftime(format_, time.localtime(time.time()))


def send_data(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = ('', port)
    server.bind(address)
    server.listen(2)
    try:
        connection, address = server.accept()
        sniff(iface="p2p4", lfilter=lambda r: r.sprintf("{Raw:%Raw.load%}") and r.sprintf("{IP:%IP.dst%}"),
              prn=lambda x: handler(x, connection))
    except Exception as e:
        print e


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="read net card data to a MySQL database.")
    ap.add_argument('--port', '-p', default="50003", type=int, help="emit netflow handled data to tcp port")
    args = ap.parse_args()

    if args.port < 30001 or args.port > 65535:
        ap.exit(-1, "error: port must be 30001-65535")

    send_data(args.port)


