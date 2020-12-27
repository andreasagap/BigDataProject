import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, desc, lit, monotonically_increasing_id, sum}

import scala.collection.mutable

class TaskTwo extends Serializable{


  def start(pointsDF: DataFrame, ss: SparkSession): DataFrame ={
    val calculator = new DominanceCalculator()
    val df1 = pointsDF.withColumnRenamed("value", "pointsRow1")
    val df_1 = df1.select("*").withColumn("id", monotonically_increasing_id())

    val df2 = pointsDF.withColumnRenamed("value", "pointsRow2")
    val df_2 = df2.select("*").withColumn("id", monotonically_increasing_id())
    import ss.implicits._
//    val sqlContext = ss.sqlContext
//    df_1.createOrReplaceTempView("df_1")
//    df_2.createOrReplaceTempView("df_2")
    val df =
      df_1.as("df1").crossJoin(
        df_2.as("df2")
      ).filter(
        ($"df1.id" =!= $"df2.id") && ($"df2.id" > $"df1.id")
      )

//    val df = sqlContext.sql(
//      "SELECT DISTINCT(*) " +
//        "FROM df_1 JOIN df_2" +
//        " WHERE df_1.id < df_2.id")

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

    }).select($"_1._1".as("point"), $"_1._2".as("key"))

    //
//
    val dominanceDF = comparisonDataframe
      .filter($"key" notEqual  "-").withColumn("value", lit(1)).groupBy("key")
  .agg(sum("value"))
    //      .count()
//      .sort(desc("count"))

    dominanceDF.show(5)
    return pointsDF
  }

}
