package exercises

import org.apache.spark._
/**
 * Created by zhoufeng on 15/11/5.
 */
object BasicAvgMapPartitions {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x if x >= 1 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "AvgMapPartitions")
    val data = sc.parallelize(1 to 100)
    val sum = data.mapPartitions(partitionCtr).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val avg = sum._1 / sum._2.toFloat
    println("avg is " + avg)
  }

  def partitionCtr(input: Iterator[Int]): Iterator[(Int, Int)] = {
    val sum = input.foldRight((0, 0))((x, y) => (y._1 + x, 1 + y._2))
    Iterator(sum)
  }
}
