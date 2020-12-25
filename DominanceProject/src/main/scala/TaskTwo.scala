import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, monotonically_increasing_id}

import scala.collection.mutable

class TaskTwo extends Serializable{
  def start(pointsDF: DataFrame, ss: SparkSession): DataFrame ={
    val calculator = new DominanceCalculator()
    val df1 = pointsDF.withColumnRenamed("value", "pointsRow1")
    val df_1 = df1.select("*").withColumn("id", monotonically_increasing_id())

    val df2 = pointsDF.withColumnRenamed("value", "pointsRow2")
    val df_2 = df2.select("*").withColumn("id", monotonically_increasing_id())


    val sqlContext = ss.sqlContext
    import ss.implicits._
    df_1.createOrReplaceTempView("df_1")
    df_2.createOrReplaceTempView("df_2")


    val df = sqlContext.sql(
      "SELECT DISTINCT(*) " +
        "FROM df_1 LEFT OUTER JOIN df_2" +
        " WHERE df_1.id < df_2.id")



    val comparisonDataframe = df.map(row => {
      val point1 = row.getAs[mutable.WrappedArray[Double]](0).toArray
      val point2 = row.getAs[mutable.WrappedArray[Double]](2).toArray
      val dominant = calculator.isDominatedTask2(point1, point2)
      val id = dominant._1._2
      var key = "-"
      if(id == 1){
        key = row.get(1).toString
      } else if (id == 2){
        key = row.get(3).toString
      }
      Tuple1(dominant._1._1, key)

    })


    var finaldf = comparisonDataframe.select($"_1._1".as("point"), $"_1._2".as("key"))


    finaldf
      .filter($"key" notEqual  "-")
      .groupBy("key", "point")
      .count()
      .sort(desc("count"))

    return finaldf
  }

}
