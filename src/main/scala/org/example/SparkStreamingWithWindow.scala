package org.example

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

object SparkStreamingWithWindow extends App {
  val conf = new SparkConf().setAppName("SparkStreamingWithWindow").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val streamRDD = ssc.socketTextStream("127.0.0.1", 2222, StorageLevel.MEMORY_ONLY)
  val wordCounts = streamRDD.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(30), Seconds(10))

  wordCounts.print()
  ssc.start()
  ssc.checkpoint(".")
  ssc.awaitTermination()
}
