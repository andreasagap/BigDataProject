import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, concat_ws, count, desc, explode, lit, monotonically_increasing_id, row_number, size, udf, when}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.split


import scala.util.control.Breaks.break

object task2 extends Serializable {

  def isDominated(x: Array[Double], y:Array[Double]): Boolean = {
    return isSmaller(x,y) & isSmallerEqual(x,y)
  }
  def isDominatedTask2(row1: Array[Double], row2: Array[Double]) : Tuple1[(Array[Double], Int)]= {
    if (isSmaller (row1, row2) & isSmallerEqual (row1, row2) ) {
      Tuple1(row1, 1)
    }
    else if (isSmaller (row2, row1) & isSmallerEqual (row2, row1) ) {
      Tuple1(row2, 2)
    }
    else{
      Tuple1(Array[Double](), 0)
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


  def main(args : Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "DominanceProject")
    val ss = SparkSession.builder().appName("DataSet Test").master("local[*]").getOrCreate()
    val textRDD = sc.textFile("correlated_dim_2_nsamples_5000.txt")

    val splitRDD = textRDD.map(line => line.split(" ").map(_.toDouble).toList)
    splitRDD.take(3).foreach(println)

    // This import is needed
    import ss.implicits._

    // convert RDD to Dataframe
    val newDf = splitRDD.toDF("points")
    newDf.show(5)

    // get dimensions through the first DataFrame entry
    val dimensions = newDf.first().getList(0).size()

    // add column names to DataFrame
    var result = newDf.select((0 until dimensions).map(i => $"points"(i).as(s"dim_$i")): _*)
    result.show(5)

    result = result.withColumn("SUM", result.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2))
      .sort(desc("SUM"))

    result.show(5)

    result = result.withColumn("row_index", monotonically_increasing_id())
      .withColumn("R", when($"row_index" === 0, lit(1)).otherwise(0))

    result.show(4)

    var pointsDF = result.map(row => convertRowToArrayOfPoints(row, dimensions));

    pointsDF.show()

    val df1 = pointsDF.withColumnRenamed("value", "pointsRow1")
    val df_1 = df1.select("*").withColumn("id", monotonically_increasing_id())

    val df2 = pointsDF.withColumnRenamed("value", "pointsRow2")
    val df_2 = df2.select("*").withColumn("id", monotonically_increasing_id())


    val sqlContext = ss.sqlContext

    df_1.createOrReplaceTempView("df_1")
    df_2.createOrReplaceTempView("df_2")


    val df = sqlContext.sql(
      "SELECT DISTINCT(*) " +
        "FROM df_1 LEFT OUTER JOIN df_2" +
        " WHERE df_1.id < df_2.id")



    val comparisonDataframe = df.map(row => {
      val point1 = row.getAs[mutable.WrappedArray[Double]](0).toArray
      val point2 = row.getAs[mutable.WrappedArray[Double]](2).toArray
      val dominant = isDominatedTask2(point1, point2)
      val id = dominant._1._2
      var key = "0"
      if(id == 1){
        key = row.get(1).toString
      } else if (id == 2){
        key = row.get(3).toString
      }else{
        key = "0"
      }
      Tuple1(dominant._1._1, key)

    })


    var finaldf = comparisonDataframe.select($"_1._1".as("point"), $"_1._2".as("key"))
    finaldf.show()


    finaldf
      .groupBy("key", "point")
      .count()
      .sort(desc("count"))
      .filter($"key" notEqual  "0")
      .show()

  }

}