import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc, lit, monotonically_increasing_id, size, when}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.int2bigDecimal
object TaskOne {

  def isDominated(x: Array[Double], y:Array[Double]): Boolean = {
    return isSmaller(x,y) & isSmallerEqual(x,y)
  }

  def isSmaller(x: Array[Double], y:Array[Double]):Boolean = {
    val size = x.length
    var flag = false
    var i = 0
    for (i <-0 to size -1) {
      if (x(i) < y(i))
        flag = true
    }
    return flag
  }

  def isSmallerEqual(x: Array[Double], y:Array[Double]):Boolean = {
    val size = x.length
    var flag = true
    var i = 0
    for (i <- 0 to size - 1) {
      if (x(i) > y(i))
        flag = false
    }
    return flag
  }
  def SFSkylineCalculation(dataframe: DataFrame,dimensions: Int,ss: SparkSession):DataFrame = {
    import ss.implicits._


//    dataframe.foreach { row =>
//      row
//    }
    return dataframe
  }


  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]" , "DominanceProject")
    val ss = SparkSession.builder().appName("DataSet Test")
      .master("local[*]").getOrCreate()
    val fileRDD = sc.textFile("C:\\\\Users\\\\Andreas\\\\Desktop\\\\BigDataProject\\\\Datasets\\\\normal_dim_2_nsamples_5000.txt")
    val splitRDD = fileRDD.map(line => line.split(" ").map(_.toDouble).toList)
    splitRDD.take(3).foreach(println)
    // This import is needed
    import ss.implicits._
    val newDf = splitRDD.toDF("points")
    val dimensions = newDf.limit(1).select("points").collectAsList().get(0).getList(0).size()
    var result = newDf.select((0 until dimensions).map(i => $"points"(i).as(s"dim_$i")): _*)

    result = result.withColumn("SUM", result.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2))
      .sort(desc("SUM"))

    result = result.withColumn("row_index", monotonically_increasing_id())
      .withColumn("R", when($"row_index" === 0, lit(1)).otherwise(0))
    //result = result.withColumn("T", lit(0))
    result.show(4)
    val mainMemorySkylines = SFSkylineCalculation(result,dimensions,ss)
    mainMemorySkylines.take(2).foreach(println)
  }
}