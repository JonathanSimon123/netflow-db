#!/usr/local/python2.7
# netflow.py
# Decodes netflow messages and stores them in a database

from __future__ import print_function
import Queue
import struct
from datetime import datetime
import sys
import socket
import thread


STRUCT_LEN = {
    1: "B",
    2: "H",
    4: "I",
    8: "Q"
}


def unpack(data):
    ln = len(data)
    if ln == 0:
        raise Exception("data is length 0")
    fmt = 'B'
    if ln in STRUCT_LEN:
        fmt = STRUCT_LEN[ln]
    retval = struct.unpack('>%s' % fmt, data)
    if len(retval) == 1:
        retval = retval[0]
    return retval


class NetflowRecord(object):
    def __init__(self):
        self.data = {}
        self['version'] = 0
        self['reporter'] = None
        self['src_id'] = None
        self['time_offset'] = None

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, item, value):
        self.data[item] = value

    def __iter__(self):
        return self.data.__iter__()

    def keys(self):
        return self.data.keys()

    @staticmethod
    def decode(data, addr):
        return []


NETFLOW_REC_10 = {
    "IP ToS": 1,
    "Protocol": 1,
    "SrcPort": 2,
    "DstPort": 2,
    "ICMP Type": 2,
    "InputInt": 4,
    "Vlan Id": 2,
    "SrcMask": 1,
    "DstMask": 1,
    "SrcAS": 4,
    "DstAS": 4,
    "NextHop": 4,
    "TCP Flags": 1,
    "OutputInt": 4,
    "Octets BYTES": 8,
    "Packets": 8,
    "MinTTL": 1,
    "MaxTTL": 1,
    "StartTime": 8,
    "EndTime": 8,
    "Flow End Reason": 1,
    "Dot1q Vlan Id": 2,
    "Dot1q Customer Vlan Id": 2,
}

flag_ord = [
    "IP ToS",
    "Protocol",
    "SrcPort",
    "DstPort",
    "ICMP Type",
    "InputInt",
    "Vlan Id",
    "SrcMask",
    "DstMask",
    "SrcAS",
    "DstAS",
    "NextHop",
    "TCP Flags",
    "OutputInt",
    "Octets BYTES",
    "Packets",
    "MinTTL",
    "MaxTTL",
    "StartTime",
    "EndTime",
    "Flow End Reason",
    "Dot1q Vlan Id",
    "Dot1q Customer Vlan Id",
]


def start_tcp_server(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (ip, port)
    sock.bind(server_address)
    try:
        sock.listen(1)
    except socket.error as e:
        print("fail to listen on port %s" % e)
        sys.exit(1)
    while True:
        client, addr = sock.accept()
        print('having a connection')
        client.close()


def accept(data_length, q):
    while True:
        data, addr = s.recvfrom(1024)

        pa = {}
        if len(data) != data_length:
            continue

        pa["version"] = unpack(data[0:2])
        pa["Length"] = unpack(data[2:4])
        seconds = unpack(data[4:8])

        pa["Timestamp"] = seconds
        # pa["datatime"] = datetime.fromtimestamp(seconds).strftime("%A, %B %d, %Y %I:%M:%S")
        pa["datatime"] = datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M:%S")
        pa["FlowSequence"] = unpack(data[8:12])
        pa["Observation Domain Id"] = unpack(data[12:16])

        # flow length 84
        pa["FlowSet Id"] = unpack(data[16:18])
        pa["FlowSet Length"] = unpack(data[18:20])
        pa["SrcAddr"] = "{}.{}.{}.{}".format(unpack(data[20:21]), unpack(data[21:22]), unpack(data[22:23]),
                                             unpack(data[23:24]))
        pa["DstAddr"] = "{}.{}.{}.{}".format(unpack(data[24:25]), unpack(data[25:26]), unpack(data[26:27]),
                                             unpack(data[27:28]))
        x = 28
        for i in range(len(flag_ord)):
            filed = flag_ord[i]
            if filed == "NextHop":
                xx = ""
                for _ in range(4):
                    xx += str(unpack(data[x:x + 1])) + "."
                    x = x + 1
                pa[filed] = "{}".format(xx)
                continue
            end = x + NETFLOW_REC_10[filed]
            pa[filed] = "{}".format(unpack(data[x:end]))
            x = end
        print("{} {} {}".format(pa["datatime"], pa["DstAddr"], pa["Octets BYTES"]))
        q.put("{} {} {}".format(pa["datatime"], pa["DstAddr"], pa["Octets BYTES"]))


def conn_thread(connection, add, que):
    print("address:{} connected".format(add))
    while True:
        if not que.empty():
            data = que.get()
            connection.sendall(data + "\r\n")


class EmitDataToTcpPort(object):

    @staticmethod
    def do(receive_udp_port, emit_tcp_port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.bind(('', int(receive_udp_port)))

        q = Queue.Queue()
        thread.start_new_thread(accept, (100, q))
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        address = ('', int(emit_tcp_port))
        server.bind(address)
        server.listen(4)

        while True:
            connection, address = server.accept()
            thread.start_new_thread(conn_thread, (connection, address, q))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python netflow_v10.py <netflow_port> <port>", file=sys.stderr)
        exit(-1)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.bind(('', int(sys.argv[1])))

    q = Queue.Queue()
    thread.start_new_thread(accept, (100, q))
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = ('', int(sys.argv[2]))
    server.bind(address)
    server.listen(4)

    while True:
        connection, address = server.accept()
        thread.start_new_thread(conn_thread, (connection, address, q))
