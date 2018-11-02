import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

val ssc = new StreamingContext(sc, Seconds(2))
val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

case clas Record(word: String)

val words = lines.flatMap(_.split(" ")

words.foreachRDD {
    (rdd: RDD[String], time:time) =>
    val wordsDataFrame = rdd.map(w => Record(w)).toDF()
    wordsDataFrame.registerTempTable("words")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word")
    println(s"========= $time =========")
    wordCountsDataFrame.show()
}