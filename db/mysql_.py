#!/usr/bin/env python
# coding=utf-8
import logging
import mysql.connector


def singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@singleton
class DB(object):
    def __init__(self, db_user, db_password, db_host, db_port, db_name):
        self.conn = None
        self.config = {
            'user': db_user,
            'password': db_password,
            'host': db_host,
            'port': db_port,
            'database': db_name,
            'charset': 'utf8'
        }

    def connect(self):
        if self.conn is None or not self.conn.is_connected():
            try:
                self.conn = mysql.connector.connect(**self.config)
            except Exception as e:
                logging.error(e)
                raise e

    def create_tables(self):
        if self.conn is not None:
            query = "show tables;"
            c = self.conn.cursor()
            c.execute(query)
            tables = {r[0] for r in c}
            c.close()

            if 'netflow' not in tables:
                c = self.conn.cursor()
                query = """
                 CREATE TABLE `netflow`  (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `ip` varchar(80) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
                  `seconds_sum` int(11) NULL DEFAULT NULL,
                  `time` varchar(60) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
                  PRIMARY KEY (`id`) USING BTREE
                )
                """
                c.execute(query)
                self.conn.commit()
                c.close()

    def disconnect(self):
        if self.conn is not None:
            self.conn.disconnect()
            self.conn = None

    def select(self, sql):
        logging.debug("sql={}".format(sql))
        try:
            cur = self.conn.cursor()
            count = cur.execute(sql)
            return count, cur.fetchall()
        except Exception as e:
            logging.error(e)

    def insert(self, sql):
        logging.debug("sql={}".format(sql))
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            logging.error(e)

    def update(self, sql):
        logging.debug("sql={}".format(sql))
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            logging.error(e)

    def delete(self, sql):
        logging.debug("sql={}".format(sql))
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            logging.error(e)


if __name__ == "__main__":
    # test
    mysql_client = DB("root", "password", "localhost", "3366", "netflow")
    mysql_client.connect()
    mysql_client.create_tables()
    query = "show tables"
    print(mysql_client.select(query))
    sql = '''insert into netflow  (ip, seconds_sum, time,date)values('103.88.35.67', 96, '21:50:55', '2017-04-03')'''
    mysql_client.insert(sql)
