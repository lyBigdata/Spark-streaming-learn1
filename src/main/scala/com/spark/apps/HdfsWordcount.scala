package com.spark.apps

import org.apache.spark.SparkConf

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by liuyu on 8/23/15.
 */

object HdfsWordcount {
 def main(args:Array[String]): Unit ={
   if (args.length < 1) {
     System.err.println("Usage: HdfsWordCount <directory>")
     System.exit(1)
   }

   val sparkConf = new SparkConf().setAppName("HdfsWordCount")

   //创建StreamingContext
   val ssc = new StreamingContext(sparkConf, Seconds(5))
   val lines = ssc.socketTextStream("localhost",9999)

   val words = lines.flatMap(_.split(" "))
   val wordCounts = words.map(x => (x, 1))
   wordCounts.print()

   ssc.start()
   ssc.awaitTermination()
 }
}
