import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, lit, size}

import scala.collection.mutable
import scala.math.BigDecimal.int2bigDecimal
object TaskOne {


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
    val maxCols = newDf.limit(1).select("points").collectAsList().get(0).getList(0).size()
    var result = newDf.select((0 until maxCols).map(i => $"points"(i).as(s"dim_$i")): _*)

    result = result.withColumn("SUM", result.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2))
    result = result.sort(desc("SUM"))

    result = result.withColumn("R", lit(0))
    result = result.withColumn("T", lit(0))
    result.show(5)
  }
}