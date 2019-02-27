package PipeObjects

//Import Project classes

//Import Spark packages
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import com.semantix.aijusProd.VariablesYAML._
//  Transform predicted column to original scale
class Unscale(override val uid: String) extends Transformer with DefaultParamsWritable{
  var originalCol: String = _
  var outputCol: String = _

  def this() = this(Identifiable.randomUID("unscale"))

  def copy(extra: ParamMap): Unscale = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  def setInputCol(column: String): Unscale = {
    this.originalCol = column
    this
  }

  def setOutputCol(column: String): Unscale = {
    this.outputCol = column
    this
  }

  override def transform(df: Dataset[_]): DataFrame = {
//    Calculates mean and standard deviation of original columns and apply transformation to predicted column
    val mu = df.select(avg(originalCol)).first().getDouble(0)
    val sd = df.select(stddev(originalCol)).first().getDouble(0)

    df.withColumn(outputCol+unscaledSuffix,col(outputCol)*sd + mu)
  }
}
