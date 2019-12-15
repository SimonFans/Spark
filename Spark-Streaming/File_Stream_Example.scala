Goal:
Get a streaming word count statisitcs on incoming files, once you start to run the spark-streaming job, you can open
another ssh connection, target to logfile and create new file there with some words, then you will see the counting result.

// create a path for streaming test

[xzhao@xxxxx streaming]$ pwd
/home/xzhao/mycode/streaming

// List files 

[xzhao@xxxxx streaming]$ ll
total 20
drwxrwxr-x 2 xzhao xzhao 4096 Dec 15 22:45 logfile
drwxrwxr-x 3 xzhao xzhao 4096 Dec 15 22:35 project
-rw-rw-r-- 1 xzhao xzhao  146 Dec 15 22:39 simple.sbt
drwxrwxr-x 3 xzhao xzhao 4096 Dec 15 22:23 src
drwxrwxr-x 4 xzhao xzhao 4096 Dec 15 22:35 target

// create sbt file <current spark version is 2.0.1, scala version is 2.11.0> and run sbt package

[xzhao@xxxxx streaming]$ cat simple.sbt 
name := "Simple Project"
version := "2.0.1"
scalaVersion := "2.11.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.1"

~/sbt/bin/sbt package

// streaming source code:

import org.apache.spark._
import org.apache.spark.streaming._
object WordCountStreaming {
   def main(args:Array[String]){
       val sparkConf = new SparkConf().setAppName("Word Count Streaming").setMaster("local[2]")
       val ssc = new StreamingContext(sparkConf, Seconds(2))
       val lines = ssc.textFileStream("file:///home/xzhao/mycode/streaming/logfile")
       val words = lines.flatMap(_.split(" "))
       val wordsCounts = words.map(x=>(x,1)).reduceByKey(_+_)
       wordsCounts.print()
       ssc.start()
       ssc.awaitTermination()
   }
}

// Run Spark-submit job

[xzhao@xxxxx streaming]$ spark-submit --class "WordCountStreaming" ~/mycode/streaming/target/scala-2.11/simple-project_2.11-2.0.1.jar 


// Result:

-------------------------------------------
Time: 1576449936000 ms
-------------------------------------------

-------------------------------------------
Time: 1576449938000 ms
-------------------------------------------

{"level": "INFO ", "timestamp": "2019-12-15 22:45:40,101", "classname": "com.hadoop.compression.lzo.GPLNativeCodeLoader", "body": "Loaded native gpl library"}
{"level": "WARN ", "timestamp": "2019-12-15 22:45:40,102", "classname": "com.hadoop.compression.lzo.LzoCompressor", "body": "java.lang.NoSuchFieldError: lzoCompressLevelFunc"}
{"level": "ERROR", "timestamp": "2019-12-15 22:45:40,103", "classname": "com.hadoop.compression.lzo.LzoCodec", "body": "Failed to load/initialize native-lzo library"}
-------------------------------------------
Time: 1576449940000 ms
-------------------------------------------
(English,1)
(likes,2)
(fish,1)
(very,1)
(eatting,1)
(knows,1)
(well,1)
(homework,1)
(He,3)
(doing,1)

-------------------------------------------
Time: 1576449942000 ms
-------------------------------------------

-------------------------------------------
Time: 1576449944000 ms
-------------------------------------------




