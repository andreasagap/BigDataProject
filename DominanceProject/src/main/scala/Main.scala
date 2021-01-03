import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, desc, size}

import scala.collection.mutable

object Main extends Serializable{

  def main(args : Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val topK = 5
    //Logger.getLogger("akka").setLevel(Level.OFF)
    // val path = "C:\\\\Users\\\\Andreas\\\\Desktop\\\\BigDataProject\\\\Datasets\\\\uniform_dim_2_nsamples_5000.txt"
    val path = "C:\\\\Users\\\\Andreas\\\\Desktop\\\\BigDataProject\\\\Datasets\\\\uniform_dim_2_nsamples_5000.txt"
    val sc = new SparkContext("local[2]", "DominanceProject")
    val ss = SparkSession.builder().appName("DataSet Test")
      .master("local[2]").getOrCreate()
    import ss.implicits._
    val utils = new Utils()
    val task1 = new TaskOne()
    val task2 = new TaskTwo()
    val task3 = new TaskThree()
    val tuple = utils.preprocessing(sc,path,ss)
    val result = tuple._1
    val dimensions = tuple._2
    val startSkyline = System.nanoTime()
    val pointsDF = result.map(row => utils.convertRowToArrayOfPoints(row, dimensions))
    val skylinePartitions = pointsDF.mapPartitions(task1.start);
    val skylineDF = task1.start(skylinePartitions.collect().toIterator)
    val arraySkyline = task1.printResult(skylineDF.toArray)
    val endSkyline = System.nanoTime()

    println("Time to find skyline: " + TimeUnit.SECONDS.convert(endSkyline-startSkyline, TimeUnit.NANOSECONDS) + "s")
//    val startDominance = System.nanoTime()
//    val scoreDominanceDF = task2.start(pointsDF.toDF(),ss)
//    //scoreDominanceDF.show(10)
//    val endDominance = System.nanoTime()
//    println("Time to find dominance: " + TimeUnit.SECONDS.convert(endDominance-startDominance, TimeUnit.NANOSECONDS) + "s")
//    scoreDominanceDF.coalesce(1)
//      .write.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("/dominance.csv")
    //task3.start(skylineDF.toArray,scoreDominanceDF,ss)

    println("-------------------TASK 3-------------------")
    val startTask3 = System.nanoTime()
    task3.start(arraySkyline,pointsDF.toDF(),ss,topK)
    val endTask3 = System.nanoTime()
    println("Time to find dominance: " + TimeUnit.SECONDS.convert(endTask3-startTask3, TimeUnit.NANOSECONDS) + "s")

  }
}
