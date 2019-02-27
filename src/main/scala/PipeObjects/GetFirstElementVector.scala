package PipeObjects

//Import Project classes
import aijusProd.PosProcessing._

//Import Spark packages
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

//Gets the first element of a ml vector
class GetFirstElementVector(override val uid: String) extends Transformer with MyHasInputCol with MyHasOutputCol with
DefaultParamsWritable with DefaultParamsReadable[GetFirstElementVector] {

  def this() = this(Identifiable.randomUID("getfirstelementvector"))

  def copy(extra: ParamMap): GetFirstElementVector = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField(getOutputCol, DoubleType, nullable = false))
  }

  def transform(df: Dataset[_]): DataFrame = {
    df.withColumn(getOutputCol,
      getElement(
        col(getInputCol),
        lit(0)
      )
    )
  }

}


