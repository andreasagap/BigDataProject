import java.io.{File, FileWriter}
import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, desc, size}

import scala.collection.mutable

object Main extends Serializable {

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val topK = 5
    //Logger.getLogger("akka").setLevel(Level.OFF)

    val listPath = getListOfFiles("../Datasets/FinalDatasets/" ).map(_.getName)
   // print(listPath)
    val sc = new SparkContext("local[2]", "DominanceProject")
    val ss = SparkSession.builder().appName("DataSet Test").master("local[2]").getOrCreate()

    val utils = new Utils()
    val task1 = new TaskOne()
    val task2 = new TaskTwo()
    val task2grid = new TaskTwoGrid()
    val task3 = new TaskThree()
    import ss.implicits._
    for ( pathTxt <- listPath ) {

//      val file_str = pathTxt.toString
//      print(file_str)

      val pathSplitArray = pathTxt.split("_")

      val path = "../Datasets/FinalDatasets/" + pathTxt
      val tuple = utils.preprocessing(sc, path, ss)
      val result = tuple._1
      val dimensions = tuple._2
      val startSkyline = System.nanoTime()
      val pointsDF = result.map(row => utils.convertRowToArrayOfPoints(row, dimensions))
      val rdd3 = pointsDF.coalesce(5)
//      println("Repartition size : "+rdd3.rdd.partitions.size)
      val skylinePartitions = rdd3.mapPartitions(task1.start).collect().toIterator


      val skylineDF = task1.start(skylinePartitions)
      val arraySkyline = task1.printResult(skylineDF.toArray)
      val endSkyline = System.nanoTime()
      val fw = new FileWriter("Task1results_"+pathSplitArray(0)+".txt", true)

      try {
        fw.write("Dims: "+pathSplitArray(2) + " Samples: " + pathSplitArray(4).split('.')(0)+"\n")
        fw.write("Size skyline: "+ arraySkyline.length+"\n")
        fw.write("Time: " + TimeUnit.SECONDS.convert(endSkyline - startSkyline, TimeUnit.NANOSECONDS) + "s\n")
        fw.write("---------Skyline points---------\n")
        for (row <- arraySkyline) {
          fw.write(row.mkString(",")+"\n")
        }
        fw.write("---------End---------\n")
      }
      finally fw.close()
      println("Time to find skyline: " + TimeUnit.SECONDS.convert(endSkyline - startSkyline, TimeUnit.NANOSECONDS) + "s")



      val fwtask2 = new FileWriter("Task2results_"+pathSplitArray(0)+".txt", true)

      val startTask2 = System.nanoTime()
      val task2_results = task2grid.start(pointsDF, ss, dimensions, topK, utils)
      val dominanceTopKArray = task2_results._1._1
      val deletedPointsCount = task2_results._1._2
      val endTask2 = System.nanoTime()
      try {
        fwtask2.write("-------------------TASK 2(GRID)-------------------\n")
        fwtask2.write("Dims: "+pathSplitArray(2) + " Samples: " + pathSplitArray(4).split('.')(0)+"\n")
        fwtask2.write("Time to find Top-K Dominant: " + TimeUnit.SECONDS.convert(endTask2 - startTask2, TimeUnit.NANOSECONDS) + "s\n")
        dominanceTopKArray.foreach(row=>{
          fwtask2.write("Point: "+row.getAs[mutable.WrappedArray[Double]](0).toArray.mkString("[", ",", "]") + ", Score: " + row.get(2)+"\n")
        })
        fwtask2.write("# of deleted points: "+ deletedPointsCount +"\n")

        fwtask2.write("---------End---------\n")
      }
      finally fwtask2.close()


      val fwtask3 = new FileWriter("Task3results_"+pathSplitArray(0)+".txt", true)

        val startTask3=System.nanoTime()
        val task3_results = task3.start(arraySkyline,pointsDF.toDF(),ss,topK,utils)
        val endTask3=System.nanoTime()
      try {
        fwtask3.write("----------TASK 3----------\n")

        task3_results.foreach(row=>{
          fwtask3.write("Point: "+row.get(0) + ", Score: " + row.get(1)+"\n")
        })
        fwtask3.write("Time to find Top-K Skyline: "+TimeUnit.SECONDS.convert(endTask3-startTask3,TimeUnit.NANOSECONDS)+"s")
      }
      finally fwtask3.close()
    }


    // Brute force for Task 2 to check the results of our implementation
    //val path = "uniform_dim_2_nsamples_5000.txt"
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






  }
}
