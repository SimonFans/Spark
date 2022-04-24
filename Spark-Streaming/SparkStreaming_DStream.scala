package com.atguigu.bigdata.spark.core.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.util.Random

object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      ssc.sparkContext.setLogLevel("WARN")
      val messageDS = ssc.receiverStream(new MyReceiver())
      messageDS.print()
      ssc.start()
      ssc.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
      private var flag = true
      override def onStart() = {
          new Thread(new Runnable {
            override def run(): Unit = {
              while (flag){
                val message = "Collected data: " + new Random().nextInt(10).toString()
                store(message)
                Thread.sleep(500)
              }
            }
          }).start()
        }
    override def onStop() = {
      flag = false
    }
  }
}


<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>atguigu-classes</artifactId>
        <groupId>com.atguigu.bigdata</groupId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>Spark-core</artifactId>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-yarn_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
    </dependencies>
    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
    </properties>

</project>
