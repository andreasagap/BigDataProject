import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{desc, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class TaskThree extends Serializable{


  def start(skylineArray:Array[Array[Double]],pointsDF: DataFrame,ss: SparkSession,k:Int, utils: Utils) {

    import ss.implicits._

    val calculator = new DominanceCalculator()
    val skylineDF = skylineArray.toSeq.toDF("point")

//    val skylineCount = skylineDF.union(pointsDF)
//    val df1 = skylineDF.withColumnRenamed("point", "pointsRow1")
//    val df_1 = df1.select("*").withColumn("id", monotonically_increasing_id())
//
//    val df2 = skylineCount.withColumnRenamed("point", "pointsRow2")
//    val df_2 = df2.select("*").withColumn("id", monotonically_increasing_id())
//    val df =
//      df_1.as("df1").crossJoin(
//        df_2.as("df2")
//      ).filter(
//        ($"df1.id" =!= $"df2.id") && ($"df2.id" > $"df1.id")
//      )
//
//    val comparisonDataframe = df.map(row => {
//      val point1 = row.getAs[mutable.WrappedArray[Double]](0).toArray
//      val point2 = row.getAs[mutable.WrappedArray[Double]](2).toArray
//      val dominant = calculator.isDominatedTask2(point1, point2)
//      val id = dominant._1._2
//      var key = "-"
//      if(id == 1){
//        key = row.get(1).toString
//      } else if (id == 2){
//        key = "-" //We don't need this information
//      }
//      Tuple1(dominant._1._1.mkString(","), key)
//
//    }).select($"_1._1".as("point"), $"_1._2".as("key"))
//
//    val finaldf = comparisonDataframe
//      .filter($"key" notEqual  "-")
//      .groupBy("key", "point")
//      .count()
//      .sort(desc("count"))
//    finaldf.show(k)


    // MAP PARTITION APPROACH
    val candidatePointsList =  pointsDF.rdd.map(r =>{
      val list = r.getAs[mutable.WrappedArray[Double]](0).toList
      (list(0), list(1))
    }).collect()

    val cp_list = candidatePointsList.toList
    utils.printList(cp_list)


    val dominanceDf = skylineDF.mapPartitions(iterator => {

      val res = iterator.map(row=>{
        val point = row.getAs[mutable.WrappedArray[Double]](0).toArray
        val pp = point.mkString(",")
        val count = utils.skyline_dominance_score(point, cp_list)
        (pp, count)
      })
      res
    }).select($"_1".as("point"), $"_2".as("score"))

    val finaldf = dominanceDf
          .sort(desc("score"))


    finaldf.show(k)

  }

}
