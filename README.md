# netflow-db
Save netflow messages to a database for analysis.

Supports gathering Netflow records in V10 (custom template) format. At the moment, only MySQL is supported as a back end database.

This enables users to perform pivot-table operations on the data in Excel.

Required:
* Python 2.7
* Spark 2.3
* Oracle MySQL Connector
* scapy


# spark的3种运行模式
- local：本地单进程模式，用于本地开发测试Spark代码
- standalone：分布式集群模式，Master-Worker架构，Master负责调度，Worker负责具体Task的执行
- on yarn/mesos：运行在yarn/mesos等资源管理框架之上，yarn/mesos提供资源管理，spark提供计算调度，并可与其他计算框架(如MapReduce/MPI/Storm)共同运行在同一个集群之上 (使用cloudera搭建的集群就是这种情况)


# usage
    单文件启动：
    python spark.py 
    
    建议两步启动：
    python read_data.py --port=30010
    python spark.py --port=30010
    
# 遗留问题
    1.数据量过大，不宜用mysql存
    2.使用standalong 方式作业，会出现python包找不到问题
   