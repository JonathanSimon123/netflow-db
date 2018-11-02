#!/usr/bin/env python
# coding=utf-8
import logging
import mysql.connector


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
                 DROP TABLE IF EXISTS `netflow`;
                CREATE TABLE `netflow`  (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `ip` varchar(80) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
                  `seconds_30_avg` int(11) NULL DEFAULT NULL,
                  `time` varchar(60) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
                  PRIMARY KEY (`id`) USING BTREE
                );
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
            conn.commit()
        except Exception as e:
            logging.error(e)

    def update(self, sql):
        logging.debug("sql={}".format(sql))
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logging.error(e)

    def delete(self, sql):
        logging.debug("sql={}".format(sql))
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logging.error(e)


if __name__ == "__main__":
    db = DB("root", "password", "localhost", "3366", "netflow")
    db.connect()
    query = "show tables"
    print(db.select(query))
