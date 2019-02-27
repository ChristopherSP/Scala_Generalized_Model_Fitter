package PipeObjects

import org.apache.spark.sql.DataFrame

//Auxiliary object created to transfer a variable between classes. This is done so that we don't need to recalculate
// the same table again, as it includes a distinct operation to a future join, so it can be an expensive operation
object DescribeLabels {
  var describeLabels: DataFrame = _

  def setDescribeLabels(df: DataFrame): Unit = {
    this.describeLabels = df
  }

  def getDescribeLabels: DataFrame = {
    this.describeLabels
  }
}
