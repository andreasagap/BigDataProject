import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc}

class Utils extends Serializable{


  def preprocessing(sc: SparkContext, path: String, ss: SparkSession):(DataFrame, Int) = {
    val fileRDD = sc.textFile(path)
    val splitRDD = fileRDD.map(line => line.split(" ").map(_.toDouble).toList)

    // This import is needed
    import ss.implicits._
    val newDf = splitRDD.toDF("points")
    val dimensions = newDf.limit(1).select("points").collectAsList().get(0).getList(0).size()
    var result = newDf.select((0 until dimensions).map(i => $"points"(i).as(s"dim_$i")): _*)

    result = result.withColumn("SUM", result.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2))
      .sort(desc("SUM"))
    return (result,dimensions)
  }

  def convertRowToArrayOfPoints(row: Row, dimensions: Int):Array[Double] = {
    var array_dims_row : Array[Double] = Array()
    for( w <- 0 until dimensions)
    {
      array_dims_row = array_dims_row :+ row(w).asInstanceOf[Double]
    }


    return array_dims_row
  }
}
