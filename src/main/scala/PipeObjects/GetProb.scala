package PipeObjects

//Import Project classes
import PipeObjects.DescribeLabels._

//Import Spark packages
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import com.autoML.VariablesYAML._
import com.autoML.PosProcessing._

//Gets the probability of a given label in a ml vector
class GetProb(override val uid: String) extends Transformer with MyHasInputCol with MyHasOutputCol with
  DefaultParamsWritable {

  var probIndex: Int = 0
  var probLabel: String = _

  def this() = this(Identifiable.randomUID("getprob"))

  def copy(extra: ParamMap): GetProb = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

//  Set desired label
  def setProbLabel(probLabel: String): GetProb = {
    this.probLabel = probLabel
    this
  }

  def setProbIndex(label: String): GetProb = {
//    Gets the Dictionary Table stored in the auxiliary object DescribeLabels
    val describeLabels: DataFrame = getDescribeLabels

//    Find index of the given label
    val index = describeLabels.filter(col("original_label") === label)
      .select(col("original_index"))
      .first()
      .getDouble(0)
      .toInt

//    Set and return it
    this.probIndex = index
    this
  }

  def transform(df: Dataset[_]): DataFrame = {

    setProbIndex(probLabel)

//    If the given label does not exist returns the dataframe as it is, else, returns the dataframe plus the
    // probability column
    if (df.columns.contains(getInputCol))
      df.withColumn(getOutputCol,
        getElement(
          col(getInputCol),
          lit(probIndex)
        )
      )
    else
      df.toDF()
  }
}
