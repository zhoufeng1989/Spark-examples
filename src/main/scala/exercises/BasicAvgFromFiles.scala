package exercises

import org.apache.spark._
import sys._
/**
 * Created by zhoufeng on 15/11/5.
 */
object BasicAvgFromFiles {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage [master] [input_directory] [output_directory]")
      exit(1)
    }
    val master = args(0)
    val inputDir = args(1)
    val outputDir = args(2)
    val sc = new SparkContext(master, "BasicAvgFromFiles")
    val input = sc.wholeTextFiles(inputDir)
    val sums = input.mapValues(value => {
      val nums = value.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    })
    sums.saveAsTextFile(outputDir)
  }

}
