package com.atguigu.bigdata.spark.core.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
      // Streaming Context creation needs two parameters.
      // The first parameter indicates environment configuration
      // The second parameter indicates micro-batching processing time period (in seconds)
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      val sc = ssc.sparkContext
      sc.setLogLevel("ERROR")
//      ssc.sparkContext.setLogLevel("WARN")
      // Logic implementation
      val lines = ssc.socketTextStream("localhost", 8081)
      val words = lines.flatMap(_.split(" "))
      val wordToOne = words.map((_,1))
      val wordToCount = wordToOne.reduceByKey(_+_)
      wordToCount.print()
      // SparkStreaming Receivers should run until it's told to stop
      ssc.start()
      ssc.awaitTermination()
  }
}

/*
Open socket from localhost, typing command: nc -lk 8081 then start to type words 
The wordCount program will simulate the receiver and driver, so need at least two cores to see the print message from console.
*/

/* Pom.xml , spark 3.0.0, scala 2.12.x */
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
