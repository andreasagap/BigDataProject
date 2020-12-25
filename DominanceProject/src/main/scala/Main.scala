import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, desc, size}

import scala.collection.mutable

object Main extends Serializable{

  def main(args : Array[String]): Unit = {
    //Logger.getRootLogger.setLevel(Level.INFO)
    // val path = "C:\\\\Users\\\\Andreas\\\\Desktop\\\\BigDataProject\\\\Datasets\\\\uniform_dim_2_nsamples_5000.txt"
    val path = "C:\\\\Users\\\\Andreas\\\\Desktop\\\\BigDataProject\\\\Datasets\\\\uniform_dim_2_nsamples_5000.txt"
    val sc = new SparkContext("local[2]", "DominanceProject")
    val ss = SparkSession.builder().appName("DataSet Test")
      .master("local[2]").getOrCreate()
    import ss.implicits._
    val utils = new Utils()
    val task1 = new TaskOne()
    val task2 = new TaskTwo()
    val tuple = utils.preprocessing(sc,path,ss)
    val result = tuple._1
    val dimensions = tuple._2

    val pointsDF = result.map(row => utils.convertRowToArrayOfPoints(row, dimensions));
    pointsDF.show()
    val skylineDF = task1.start(pointsDF.collect())
    val scoreDominanceDF = task2.start(pointsDF.toDF(),ss)
    scoreDominanceDF.show(10)
  }
}
