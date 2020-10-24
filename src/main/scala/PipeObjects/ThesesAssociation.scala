package PipeObjects

//import VariablesYAML.idModel6
import org.apache.spark.ml.{PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class ThesesAssociation(override val uid: String) extends Transformer with MLWritable {

  var fpgrowth: FPGrowth = _

  def this() = this(Identifiable.randomUID("thesesassociation"))

  def copy(extra: ParamMap): SentenceModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  def transform(df: Dataset[_]): DataFrame = {
    val items = df.toDF().withColumn("items_array",  split(col("items"),","))

    fpgrowth = new FPGrowth()
      .setItemsCol("items_array")
      .setMinSupport(0.2)
      .setMinConfidence(0.1)
      .setPredictionCol("PredictedSet")

    val model = fpgrowth.fit(items)

    // transform examines the input items against all the association rules and summarize the consequents as prediction
    model.transform(items)
  }

  override def write: MLWriter = fpgrowth.write
}
