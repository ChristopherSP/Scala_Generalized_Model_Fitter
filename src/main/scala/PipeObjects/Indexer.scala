//package PipeObjects
//
//import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
//import org.apache.spark.ml.feature.StringIndexer
//import org.apache.spark.ml.param.ParamMap
//import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
//import org.apache.spark.sql.{DataFrame, Dataset}
//import org.apache.spark.sql.types.StructType
//import PreProcessing.getColumnsByType
//import Variables._
//
//class Indexer(override val uid: String) extends Transformer with MyHasInputCol with MyHasOutputCol with
//  DefaultParamsWritable {
//
//
//  def this() = this(Identifiable.randomUID("indexer"))
//
//  def copy(extra: ParamMap): GetProb = {
//    defaultCopy(extra)
//  }
//
//  override def transformSchema(schema: StructType): StructType = {
//    schema
//  }
//
//  override def transform(df: Dataset[_]): DataFrame = {
//    val stringInputed = getColumnsByType(df, "StringType").filter(column => column.contains(inputedSuffix))
//
//    val categoricalIndexer = stringInputed.map(
//      column => new StringIndexer()
//        .setInputCol(column)
//        .setOutputCol(s"${column + indexSuffix}")
//        .asInstanceOf[PipelineStage]
//    )
//
//    val pipeline: Pipeline = new Pipeline().setStages(categoricalIndexer)
//
//    pipeline.fit(df).transform(df)
//  }
//
//}
