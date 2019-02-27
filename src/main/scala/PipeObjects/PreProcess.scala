package PipeObjects
//Import Project classes
import aijusProd.Variables._

//Import Spark packages
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}

//Deals with all preprocessing required to apply models
class PreProcess(numerical: Array[String], categorical: Array[String]) {
//  Vectorize numerical columns
  val numericalAssembler: Array[PipelineStage] = numerical.map(
    column => new VectorAssembler()
      .setInputCols(Array(column))
      .setOutputCol(s"${column + vectorSuffix}")
      .asInstanceOf[PipelineStage]
  )

//  Scale vectorized columns
  val numericalScaler: Array[PipelineStage] = numerical.map(
    column => new StandardScaler()
      .setInputCol(column + vectorSuffix)
      .setOutputCol(column + scaledSuffix + vectorSuffix)
      .setWithStd(true)
      .setWithMean(true)
      .asInstanceOf[PipelineStage]
  )

//  Index Labels of categorical columns
  val categoricalIndexer: Array[PipelineStage] = categorical.map(
    column => new StringIndexer()
      .setInputCol(column)
      .setOutputCol(s"${column + indexSuffix}")
      .asInstanceOf[PipelineStage]
  )

//  Unify above steps
  val steps: Array[PipelineStage] = numericalAssembler ++ numericalScaler ++ categoricalIndexer
}
