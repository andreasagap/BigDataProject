import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TaskOne {
  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]" , "DominanceProject")
    val file = sc.textFile("C:\\\\Users\\\\Andreas\\\\Desktop\\\\BigDataProject\\\\Datasets\\\\normal_dim_2_nsamples_5000.txt")
    file.collect().foreach(f=>{
      println(f)
    })

  }
}