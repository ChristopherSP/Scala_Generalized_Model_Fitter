package PipeObjects
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._

//Import Spark packages
class CleanColsModel(override val uid: String) extends Transformer with DefaultParamsWritable{
  var originalCols: Array[String] = _
  var outputCols: Array[String] = _

  def this() = this(Identifiable.randomUID("cleancolsmodel"))

  def copy(extra: ParamMap): CleanColsModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  def setInputCol(column: Array[String]): CleanColsModel = {
    this.originalCols = column
    this
  }

  def setOutputCol(column: Array[String]): CleanColsModel = {
    this.outputCols = column
    this
  }

  //  Sets which column to maintain after apply model and clean the rest
  override def transform(df: Dataset[_]): DataFrame = {
    df.select(
      (originalCols ++ outputCols).head,
      (originalCols ++ outputCols).tail: _*
    )
  }
}
