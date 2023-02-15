package sparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

/**
 * nc -lk 9999
 * 输入数据
 */
object NetworkWordCount {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.WARN)

    // 创建StreamingContext
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
      .setMaster("local[2]")//必须>1核, 否则1个线程会用来运行接收器接收数据，但没有其他的线程去处理这些数据。
    val ssc = new StreamingContext(sparkConf, Seconds(10)) //每10s一批

    // 从TCP源（主机位localhost，端口为9999）获取的流式数据, lines是 DStream
    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    // 切分为单词
    val words = lines.flatMap(_.split(" "))
    // 统计每批中的每个单词
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    // 打印到控制台
    wordCounts.print()

    // 开始计算
    ssc.start()
    // 持续运行计算
    ssc.awaitTermination()
  }
}
