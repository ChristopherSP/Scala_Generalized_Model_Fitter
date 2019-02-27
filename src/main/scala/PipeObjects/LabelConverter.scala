package PipeObjects

//Import Project classes
import PipeObjects.DescribeLabels._

//Import Spark packages
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

//  Convert predicted numerical index to a interpretable string label
class LabelConverter(override val uid: String) extends Transformer with MyHasInputCol with MyHasOutputCol with
  DefaultParamsWritable {

  var originalLabelCol: String = _
  var originalIndexCol: String = _
  var predictedIndexCol: String = _
  var predictedLabelCol: String = _

  def this() = this(Identifiable.randomUID("labelconverter"))

  def setInputCols(originalLabel: String, originalIndex: String, predictedIndex: String): LabelConverter = {
    this.originalLabelCol = originalLabel
    this.originalIndexCol = originalIndex
    this.predictedIndexCol = predictedIndex
    this
  }

  def setOutputColumn(output: String): LabelConverter = {
    this.predictedLabelCol = output
    this
  }

  def copy(extra: ParamMap): GetProb = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def transform(df: Dataset[_]): DataFrame = {
//    Creates a index:label dictionary
    val describeLabels = df.select(originalLabelCol,originalIndexCol)
      .distinct()
      .toDF()
      .withColumnRenamed(originalLabelCol, "original_label")
      .withColumnRenamed(originalIndexCol, "original_index")

//    Passes it to a auxiliary object
    setDescribeLabels(describeLabels)

//    Rename columns to join tables without duplication
    val describeLabelsRenamed = describeLabels
      .withColumnRenamed("original_label", predictedLabelCol)
      .withColumnRenamed("original_index", predictedIndexCol)

    df.join(describeLabelsRenamed,Seq(predictedIndexCol),"left")
  }
}
