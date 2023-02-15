package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
 * @author cosmoxu
 * @version output, v 0.1 2023/2/15 10:54
 */
object output {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[3]")
      .set("spark.hadoop.validateOutputSpecs", "false")//设置覆盖文件输出
    val sc = new SparkContext(conf)
    // 创建RDD
    val inputFile = "src/main/resources/word.txt"
    val textFile = sc.textFile(inputFile)
    // 算子计算
    val wordCount = textFile
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    // 输出-控制台
    wordCount.collect().foreach(println)
    // 输出-文件
    wordCount.saveAsTextFile("src/main/resources/output")
  }
}
