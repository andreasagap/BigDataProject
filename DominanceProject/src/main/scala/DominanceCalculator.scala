
class DominanceCalculator extends Serializable {
  def isDominatedTask1(x: Array[Double], y:Array[Double]): Boolean = {
    return isSmaller(x,y) & isSmallerEqual(x,y)
  }
  def isDominatedTask2(row1: Array[Double], row2: Array[Double]) : Tuple1[(Array[Double], Int)]= {
    if (isSmaller (row1, row2) & isSmallerEqual (row1, row2) ) {
      Tuple1(row1, 1)
    }
    else if (isSmaller (row2, row1) & isSmallerEqual (row2, row1) ) {
      Tuple1(row2, 2)
    }
    else{
      Tuple1(Array[Double](), 0)
    }
  }
  def isSmaller(x: Array[Double], y:Array[Double]):Boolean = {
    val size = x.length
    var flag = false
    var i = 0
    for (i <-0 until size) {
      if (x(i) < y(i))
        flag = true
    }
    return flag
  }

  def isSmallerEqual(x: Array[Double], y:Array[Double]):Boolean = {
    val size = x.length
    var flag = true
    var i = 0
    for (i <- 0 until size) {
      if (x(i) > y(i))
        flag = false
    }
    return flag
  }
}
