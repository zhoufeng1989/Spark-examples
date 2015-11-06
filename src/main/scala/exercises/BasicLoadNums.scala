package exercises

import org.apache.spark._
import sys._
/**
 * Created by zhoufeng on 15/11/6.
 */
object BasicLoadNums {
  def main(args: Array[String]): Unit = {
    println(args.toString)
    if (args.length < 2) {
      println("Usage: [master] [inputFile]")
      exit(1)
    }
    val master = args(0)
    val sc = new SparkContext(master, "BasicLoadNums")
    val inputFile = args(1)
    val input = sc.textFile(inputFile)
    val validLines = sc.accumulator(0)
    val errorLines = sc.accumulator(0)
    // Use accumulator in transformation, just for exercise!
    val count = input.flatMap(line => {
      try {
        val data = line.split(" ")
        val item = Some((data(0), data(1).toInt))
        validLines += 1
        item
      }
      catch {
        case e: java.lang.NumberFormatException => {
          errorLines += 1
          None
        }
        case e: java.lang.ArrayIndexOutOfBoundsException => {
          errorLines += 1
          None
        }
      }
    }).reduceByKey(_ + _).saveAsTextFile("output")
    println(s"Error lines: ${errorLines.value}, Valid lines: ${validLines.value}.")
  }
}
