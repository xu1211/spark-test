package sparkCore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author cosmoxu
 * @version WordCount, v 0.1 2023/2/13 18:04
 */
object WordCount {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 创建上下文
    val conf = new SparkConf().setAppName("WordCount")
      .setMaster("local[2]") //本地环境2核
    val sc = new SparkContext(conf)

    // 创建RDD
    val inputFile = "src/main/resources/word.txt"
    val textFile = sc.textFile(inputFile)

    // 算子计算
    val wordCount = textFile
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
    // 输出
    wordCount.collect().foreach(println)

    // 关闭
    sc.stop()
  }
}
