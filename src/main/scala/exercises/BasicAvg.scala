package exercises

import org.apache.spark._
/**
 * Created by zhoufeng on 15/11/5.
 */
object BasicAvg {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x if x >= 1 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvg")
    val dataRdd = sc.parallelize(1 to 100)
    val sum = dataRdd.map(x => (x, 1)).fold((0, 0))((x, y) => (x._1 + y._1, x._2 + y._2))
    val avg = sum._1 / sum._2.toFloat
    println("Avg is " + avg)
  }

}
