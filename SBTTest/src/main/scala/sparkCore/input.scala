package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author cosmoxu
 * @version 创建DDR, v 0.1 2023/2/15 10:08
 */
object input {
  def main(args: Array[String]): Unit = {
    // 创建上下文
    val conf = new SparkConf().setAppName("DDR")
      .setMaster("local[2]") //本地环境2核
    val sc = new SparkContext(conf)

    // 从对象创建ddr
    val no = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val RDD1 = sc.parallelize(no)
    RDD1.foreach {println}

    // 从ddr对象创建ddr
    val RDD2 = no.map(data => (data * 2))
    RDD2.foreach {println}

    // 从本地文件创建
    val txtRDD = sc.textFile("src/main/resources/word.txt")
    txtRDD.foreach {println}

    val jsonRDD = sc.textFile("file:///Users/yuchunxu/Documents/GitHub/xu/SBTTest/src/main/resources/word.json")
    jsonRDD.foreach {println}

    // 从hdfs文件创建
    var hFile = sc.textFile("hdfs://localhost:9000/inp")
    hFile.foreach {println}

  }
}
