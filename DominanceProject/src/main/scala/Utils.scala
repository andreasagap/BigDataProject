import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
    (result,dimensions)
  }

  def convertRowToArrayOfPoints(row: Row, dimensions: Int):Array[Double] = {
    var array_dims_row : Array[Double] = Array()
    for( w <- 0 until dimensions)
    {
      array_dims_row = array_dims_row :+ row(w).asInstanceOf[Double]
    }


    array_dims_row
  }


  def grid_assign(row: Array[Double], dimensions: Int): String = {
    var arr = ArrayBuffer[Int]()

    for (i <- 0 until dimensions) {
      val point = row(i)
      var grid_id = 0
      if (1 / point < 1.33) {
        grid_id = 4
      } else if (1 / point < 2) {
        grid_id = 3
      } else if (1 / point < 4) {
        grid_id = 2
      } else {
        grid_id = 1
      }

      arr += grid_id
    }
    arr.mkString("")

  }

  def dominated_cells_upper(c1 : String, c2: String): Boolean = {

    val dim = c1.toCharArray.length
    var bool = true
    for (i <- 0 until dim) {
      val p1 = c1.charAt(i).toInt - 48
      val p2 = c2.charAt(i).toInt - 48
      if(p1 > p2){
        bool = false
      }
    }
    bool
  }

  def dominated_cells_lower(c1 : String, c2: String): Boolean = {

    val dim = c1.toCharArray.length
    var bool = true
    for (i <- 0 until dim) {
      val p1 = c1.charAt(i).toInt - 48
      val p2 = c2.charAt(i).toInt - 48
      if(p1 >= p2){
        bool = false
      }
    }
    bool
  }

  def dominated_cells_gf(c1 : String, c2: String): Boolean = {

    val dim = c1.toCharArray.length
    var bool = true
    for (i <- 0 until dim) {
      val p1 = c1.charAt(i).toInt - 48
      val p2 = c2.charAt(i).toInt - 48
      if(p1 <= p2){
        bool = false
      }
    }
    bool
  }

  def dominated_partial(c1 : String, c2: String): Boolean = {

    val dim = c1.toCharArray.length
    var bool = true
    var count_bool = 0
    for (i <- 0 until dim) {
      val p1 = c1.charAt(i).toInt - 48
      val p2 = c2.charAt(i).toInt - 48
      if(p2 < p1){
        bool = false
      }
      if(p2 > p1){
        count_bool +=1
      }
    }
    if(count_bool == dim){
      bool = false
    }
    bool
  }

  def cell_bounds(cid : String, dim: Int, m: Map[String, Int]): ArrayBuffer[Int] = {
    import scala.collection.mutable.ListBuffer
    var list_of = new ListBuffer[Int]()

    val keys = m.keySet.toList
    val size = keys.size

    var upper_bound = - m.getOrElse(cid, 0)
    for(i <- 0 until size){
      val cc = keys(i)
      val count_cc = m.getOrElse(cc, 0);
      if(cid.toInt <= cc.toInt && dominated_cells_upper(cid, cc))
        upper_bound = upper_bound + count_cc
    }

    var lower_bound = 0
    for(i <- 0 until size){
      val cc = keys(i)
      val count_cc = m.getOrElse(cc, 0)
      if(cid.toInt <= cc.toInt && dominated_cells_lower(cid, cc)) {
        lower_bound = lower_bound + count_cc
      }
    }
    var gf = 0
    for(i <- 0 until size){
      val cc = keys(i)
      val count_cc = m.getOrElse(cc, 0)
      if(cid.toInt > cc.toInt && dominated_cells_gf(cid, cc)) {
        gf = gf + count_cc
      }

    }


    val arr = ArrayBuffer(lower_bound, upper_bound, gf)
    arr
  }

  def printList(args: List[_]): Unit = {
    args.foreach(println)
  }

  def compute_dominance_score(point: Array[Double], cid : Int, candicateList: List[(Any, Any)]): Int = {
    var count = 0
    val calculator = new DominanceCalculator()
    candicateList.foreach(tup =>{
      val p = tup._1.asInstanceOf[mutable.WrappedArray[Double]].toArray
      val cell2 = tup._2.asInstanceOf[String]
      val cell = cell2.toInt
      if(cell>= cid && dominated_partial(cid.toString, cell.toString)){
        if(calculator.isDominatedTask1(point, p)){
          count +=1
        }
      }
    })
    count
  }

  def skyline_dominance_score(point: Array[Double], candicateList: List[List[Double]]): Int = {
    var count = 0
    val calculator = new DominanceCalculator()
    candicateList.foreach(tup =>{
//      val p = tup._1.asInstanceOf[mutable.WrappedArray[Double]].toArray
//      val p = tup.productIterator.toArray
//      val cp = p.map(_.toString.toDouble)

      if(calculator.isDominatedTask1(point, tup.toArray)){
          count +=1
        }
    })
    count
  }

}
