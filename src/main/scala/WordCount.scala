import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
/**
  * \* Created with IntelliJ IDEA.
  * \* User: zhangZhigang
  * \* Date: 2017/12/14
  * \* Time: 下午4:20
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/

object WordCount {

  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("wordcount");
    val sc=new SparkContext(conf)

    val input=sc.textFile("/home/hadoop/spark_soft/helloSpark.txt")

    val lines=input.flatMap(line=>line.split(" "))
    val count=lines.map(word=>(word,1)).reduceByKey{case (x,y)=>x+y}

    val output=count.saveAsTextFile("/home/hadoop/spark_soft/helloSparkRes")

  }

}
