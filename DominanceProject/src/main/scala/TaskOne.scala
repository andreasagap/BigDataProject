import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, concat_ws, count, desc, explode, lit, monotonically_increasing_id, row_number, size, udf, when}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
class TaskOne extends Serializable{

  def init(): Unit ={

  }
  def start(points: Iterator[Array[Double]]):Iterator[Array[Double]] = {
      val pointsArray = points.toArray
      val calculator = new DominanceCalculator()
      var skyline = ArrayBuffer[Array[Double]]()
      skyline+=pointsArray(0)
    pointsArray.foreach { row => {
        var isSkyline = true
        var j=0
        while (j < skyline.length) {
          if (calculator.isDominatedTask1(row, skyline(j))) {
            skyline.remove(j)
            j-=1

          }
          else if (calculator.isDominatedTask1(skyline(j), row)) {
            isSkyline = false
            break()
          }
          j += 1
        }
        if (isSkyline) {
          skyline+=row
        }
      }

    }


    skyline.toIterator
  }

  def printResult(skyline:Array[Array[Double]]): Array[Array[Double]] ={
    println("-------------------Skyline points-------------------")
    var array :Array[Array[Double]] = Array()
    for (row <- skyline) {
      println(row.mkString(","))
      array :+= row
    }
    array
  }




}