import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, concat_ws, count, desc, explode, lit, monotonically_increasing_id, row_number, size, udf, when}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
object TaskOne extends Serializable {

  def isDominated(x: Array[Double], y:Array[Double]): Boolean = {
    return isSmaller(x,y) & isSmallerEqual(x,y)
  }
  def isDominatedTask2(row1: Array[Double], row2: Array[Double]) : Array[Double]= {
        if (isSmaller (row1, row2) & isSmallerEqual (row1, row2) ) {
          row1
      }
        else if (isSmaller (row2, row1) & isSmallerEqual (row2, row1) ) {
          row2
      }
      else{
          Array[Double] ()
        }
  }
  def isSmaller(x: Array[Double], y:Array[Double]):Boolean = {
    val size = x.length
    var flag = false
    var i = 0
    for (i <-0 until size) {
      if (x(i) < y(i))
        flag = true
    }
    return flag
  }

  def isSmallerEqual(x: Array[Double], y:Array[Double]):Boolean = {
    val size = x.length
    var flag = true
    var i = 0
    for (i <- 0 until size) {
      if (x(i) > y(i))
        flag = false
    }
    return flag
  }

  def convertRowToArrayOfPoints(row: Row, dimensions: Int):Array[Double] = {
    var array_dims_row : Array[Double] = Array()
    for( w <- 0 until dimensions)
    {
      array_dims_row = array_dims_row :+ row(w).asInstanceOf[Double]
    }


    return array_dims_row
  }


  def TaskOne(points: Array[Array[Double]]):ArrayBuffer[Array[Double]] = {

      var skyline = ArrayBuffer[Array[Double]]()
      skyline+=points(0)
      points.foreach { row => {
        var isSkyline = true
        var j=0
        while (j < skyline.length) {
          if (isDominated(row, skyline(j))) {
            skyline.remove(j)
            j-=1

          }
          else if (isDominated(skyline(j), row)) {
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


    return skyline
  }



  def main(args : Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]", "DominanceProject")
    val ss = SparkSession.builder().appName("DataSet Test")
      .master("local[*]").getOrCreate()
    val fileRDD = sc.textFile("C:\\\\Users\\\\Andreas\\\\Desktop\\\\BigDataProject\\\\Datasets\\\\uniform_dim_2_nsamples_5000.txt")
    val splitRDD = fileRDD.map(line => line.split(" ").map(_.toDouble).toList)

    // This import is needed
    import ss.implicits._
    val newDf = splitRDD.toDF("points")
    val dimensions = newDf.limit(1).select("points").collectAsList().get(0).getList(0).size()
    var result = newDf.select((0 until dimensions).map(i => $"points"(i).as(s"dim_$i")): _*)

    result = result.withColumn("SUM", result.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2))
      .sort(desc("SUM"))

    //
    //    result = result.withColumn("row_index", monotonically_increasing_id())
    //      .withColumn("R", when($"row_index" === 0, lit(1)).otherwise(0))


    var pointsDF = result.map(row => convertRowToArrayOfPoints(row, dimensions));
    val skyline =
      TaskOne(pointsDF.collect())

    // skyline.foreach { println }
    for (row <- skyline) {
      // row.foreach { println }
      println(row.mkString(","))
    }
    pointsDF.show()
    val df1 = pointsDF.withColumnRenamed("value", "pointsRow1")
    val df2 = pointsDF.withColumnRenamed("value", "pointsRow2")
    //    val coder: ((Array[Double],Array[Double]) => Array[Double]) = (row1: Array[Double], row2: Array[Double]) => {
    //        if (isSmaller (row1, row2) & isSmallerEqual (row1, row2) ) {
    //          row1
    //        }
    //        else if (isSmaller (row2, row1) & isSmallerEqual (row2, row1) ) {
    //          row2
    //        }
    //        else{
    //          Array[Double] ()
    //        }
    //    }
    //    val isDominatedTask2 = udf(coder)
    var df = df1.join(df2, df1("pointsRow1") < df2("pointsRow2"))
    var comparisonDataframe = df.map(row => isDominatedTask2(row.getAs[mutable.WrappedArray[Double]](0).toArray, row.getAs[mutable.WrappedArray[Double]](1).toArray))
      .withColumnRenamed("value", "Dominance")
    comparisonDataframe = comparisonDataframe.filter(size($"Dominance") > 0).withColumn("Dominance",concat_ws(",",col("Dominance")))
    //comparisonDataframe.show()
    comparisonDataframe.withColumn("point", comparisonDataframe.col("Dominance"))
      .groupBy("point")
      .count()
    .sort(desc("count"))
      .show()
//    comparisonDataframe = comparisonDataframe.groupBy("Dominance").count()
//    .sort(desc("count"))
//    comparisonDataframe.show(5)
    //df = df.join(comparisonDataframe)
      // .withColumn("Dominance", isDominatedTask2(col("pointsRow1"),$"pointsRow2"))
//    df.withColumn("Dominance", )
      //lit(isDominatedTask2(col("pointsRow1"),col("pointsRow2"))))
   // df.show()

  }

}