import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TaskTwoGrid extends Serializable{
  def init(): Unit ={

  }

  def start(pointsDF: Dataset[Array[Double]],ss: SparkSession, dimensions: Int, k: Int, utils: Utils): Tuple1[(Array[Row], Int)] ={

    import ss.implicits._

    val grid_df = pointsDF.map(row => {
      val grid_id = utils.grid_assign(row, dimensions)
      Tuple1(row, grid_id)

    }).select($"_1._1".as("point"), $"_1._2".as("grid_id"))

    //grid_df.show()


    val finaldf = grid_df
      .groupBy("grid_id")
      .count()
      .sort(desc("count"))

   // finaldf.show()


    val count_map = finaldf.select($"grid_id", $"count".cast("int")).as[(String, Int)].collect.toMap
   // println(count_map.toString())


    val bounds_df = finaldf.map(row => {
      val grid_id = row(0).toString
      val bound = utils.cell_bounds(grid_id, dimensions, count_map)
      Tuple1(grid_id, bound)
    }).select($"_1._1".as("g_id"), $"_1._2"(0).as("lower"), $"_1._2"(1).as("upper"), $"_1._2"(2).as("gf"))


//    bounds_df.show()

    val pruned_grid = bounds_df.filter(bounds_df("gf") < k)
   // pruned_grid.show()

    val deleted_cells = bounds_df.filter(bounds_df("gf") >= k)
    //deleted_cells.show()

    val deleted_points_df = deleted_cells.map(row =>{
      val cid = row(0).toString
      val count = count_map.getOrElse(cid, 0)
      (cid, count)
    }).select($"_1".as("cell_id"), $"_2".as("count"))

   // deleted_points_df.show()

    val count_deleted_points = deleted_points_df.select(col("count")).rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)
    //print(count_deleted_points)


    // val gf_map = pruned_grid.select($"g_id".cast("String"), $"gf").as[(String, Int)].collect.toMap
   // println(gf_map.toString())

    val low_map = pruned_grid.select($"g_id".cast("String"), $"lower").as[(String, Int)].collect.toMap
   // println(low_map.toString())

    val cells_after_prunning = pruned_grid.select("g_id").rdd.map(r => r(0)).collect()
    //println(cells_after_prunning.mkString(" "))

    val listOfPrunnedCells = cells_after_prunning.toList

    val candidatePoints = grid_df.filter(grid_df("grid_id").isin(listOfPrunnedCells:_*))
//    candidatePoints.show()

    val candidatePointsList =  candidatePoints.select("point", "grid_id").rdd.map(r => (r(0), r(1))).collect()
    val cp_list = candidatePointsList.toList
//    utils.printList(cp_list)

    val dominanceDf = candidatePoints.mapPartitions(iterator => {

      val res = iterator.map(row=>{
        val cell_id = row.getString(1)
        val point = row.getAs[mutable.WrappedArray[Double]](0).toArray

        val count = utils.compute_dominance_score(point, cell_id.toInt, cp_list)

        val lower_bound = low_map.getOrElse(cell_id, 0)

        val count_total = count + lower_bound

        (point, cell_id, count_total)
      })
      res
    }).select($"_1".as("point"), $"_2".as("cell_id"), $"_3".as("score"))

    Tuple1(dominanceDf.sort(desc("_3")).limit(10).collect(), count_deleted_points)

  }

}
