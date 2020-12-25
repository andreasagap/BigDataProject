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
  def start(points: Array[Array[Double]]):ArrayBuffer[Array[Double]] = {

      val calculator = new DominanceCalculator()
      var skyline = ArrayBuffer[Array[Double]]()
      skyline+=points(0)
      points.foreach { row => {
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

    printResult(skyline)
    return skyline
  }

  def printResult(skyline:ArrayBuffer[Array[Double]]): Unit ={
    for (row <- skyline) {
      // row.foreach { println }
      println(row.mkString(","))
    }
  }




}