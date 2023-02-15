package sparkCore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author cosmoxu
 * @version Parallelized, v 0.1 2023/2/14 19:45
 */
object Parallelized {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // parallelize 并手动设置分区数
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data, 4)

    val inputFile = "src/main/resources/word.txt"
    var textRDD = sc.textFile(inputFile, 2)
    textRDD = textRDD.repartition(100)

    var wordRDD = textRDD.flatMap(line => line.split(" "))

    // 缓存
    //    wordRDD.cache()
    wordRDD.persist(StorageLevel.MEMORY_ONLY)//设置缓存级别
    println("缓存word:")
    wordRDD.foreach(println)

    // 通过缓存数据筛选, 不会重新进行flatMap操作
    val numAs = wordRDD.filter(line => line.contains("a")).count()
    println(s"Lines with a: $numAs")
    val numBs = wordRDD.filter(line => line.contains("b")).count()
    println(s"Lines with b: $numBs")

    val count = wordRDD.map(word => (word, 1)).reduceByKey((a, b) => a + b)

    val a = count.sortByKey().groupByKey()
    a.foreach(println)
  }
}
