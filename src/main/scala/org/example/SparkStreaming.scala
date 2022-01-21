package org.example

import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkStreaming extends App {
  val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val streamRDD = ssc.socketTextStream("127.0.0.1", 2222)
  val wordCounts = streamRDD.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_+_)

  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
}
