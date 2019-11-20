1. local file system read & write:

val textFile = sc.textFile("file:///usr/local/spark/word.txt")  // default partition is 1
val textFile = sc.textFile("file:///usr/local/spark/word.txt",2) // if need partition when file is big

textFile.first()  // lazy till you run the action command, it will start to run & check if file exists

textFile.saveAsTextFile("file:///usr/local/spark/writeback.txt")

cd /usr/local/spark/writeback.txt
ls
part-00000
_SUCCESS

// 当再次使用，只需写到目录名，自动加载此目录下所有文件
val textFile = sc.textFile("file:///usr/local/spark/writeback.txt")

2. HDFS read & write

val textFile = sc.textFile("hdfs://localhost:9000/usr/local/spark/word.txt")
val textFile = sc.textFile("/usr/local/spark/word.txt")

textFile.first()

textFile.saveAsTextFile("/usr/local/spark/writeback.txt")

3. JSON file read & write

{"name":"M"}
{"name":"A","age":30}

val jsonStr = sc.textFile("file:///usr/local/spark/examples/src/main/resources/people.json")
jsonStr.foreach(println)

// parse JSON
import scala.util.parsing.json.JSON
inputFile = "file:///usr/local/spark/examples/src/main/resources/people.json"
val jsonStr = sc.textFile(inputFile)
val result = jsonStr.map(s=> JSON.parseFull(s))

