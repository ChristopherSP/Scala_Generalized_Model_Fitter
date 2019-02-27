//package PipeObjects
//
//import aijusProd.Variables.{dependentScaledVecCol, dependentVecCol}
//import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
//import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
//import org.apache.spark.ml.param.ParamMap
//import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
//import org.apache.spark.sql.{DataFrame, Dataset}
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.functions._
//import aijusProd.Variables._
//import aijusProd.PreProcessing.getColumnsByType
//
//class Scale(override val uid: String) extends Transformer with MyHasInputCol with MyHasOutputCol with
//  DefaultParamsWritable {
//
//
//  def this() = this(Identifiable.randomUID("scale"))
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
//    //val numerical = getColumnsByType(df, "DoubleType").filter(column => !column.contains(inputedSuffix))
//    val numericalInputed = getColumnsByType(df, "DoubleType").filter(column => column.contains(inputedSuffix))
//
//    val numericalAssembler = numericalInputed.map(
//      column => new VectorAssembler()
//        .setInputCols(Array(column))
//        .setOutputCol(s"${column + vectorSuffix}")
//        .asInstanceOf[PipelineStage]
//    )
//
//    val numericalScaler = numericalInputed.map(
//      column => new StandardScaler()
//        .setInputCol(column + vectorSuffix)
//        .setOutputCol(column + scaledSuffix)
//        .setWithStd(true)
//        .setWithMean(true)
//        .asInstanceOf[PipelineStage]
//    )
//
//    val pipeline: Pipeline = new Pipeline().setStages(numericalAssembler ++ numericalScaler)
//    //val keep = df.columns.filter(column => column.contains(vectorSuffix))
//
//    pipeline.fit(df).transform(df).toDF()//.select(keep.head,keep.tail:_*)
//
//  }
//}
