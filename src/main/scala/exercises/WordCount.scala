package exercises

import org.apache.spark._
/**
 * Created by zhoufeng on 15/11/2.
 */
object WordCount {
  def main() (args: Array[String]): Unit = {
    val master = args.length match {
      case x if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "WordCount")
    val input = args.length match {
      case x if x > 1 => sc.textFile(args(1))
      case _ => sc.parallelize(List("hello", "the World"))
    }
    val words = input.flatMap(line => line.split(" "))
    args.length match {
      case x if x > 2 => {
        val counts = words.map(word => (word, 1)).reduceByKey((x, y) => x + y)
        counts.saveAsTextFile(args(2))
      }
      case _ => {
        val wc = words.countByValue()
        println(wc.mkString(","))
      }
    }
  }
}
