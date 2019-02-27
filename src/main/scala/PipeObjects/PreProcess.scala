package PipeObjects
//Import Project classes

//Import Spark packages
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import com.semantix.aijusProd.VariablesYAML._
//Deals with all preprocessing required to apply models
class PreProcess(numerical: Array[String], categorical: Array[String]) {
//  Vectorize numerical columns
  println("####################\n\n####################\nNumerical " +
    "Assembler\n####################\n####################")
  val numericalAssembler: Array[PipelineStage] = numerical.map(
    column => new VectorAssembler()
      .setInputCols(Array(column))
      .setOutputCol(s"${column + vectorSuffix}")
      .asInstanceOf[PipelineStage]
  )

//  Scale vectorized columns
  println("####################\n\n####################\nNumerical Scaler\n####################\n####################")
  val numericalScaler: Array[PipelineStage] = numerical.map(
    column => new StandardScaler()
      .setInputCol(column + vectorSuffix)
      .setOutputCol(column + scaledSuffix + vectorSuffix)
      .setWithStd(true)
      .setWithMean(true)
      .asInstanceOf[PipelineStage]
  )

  println("####################\n\n####################\nCategorical " +
    "Indexer\n####################\n####################")
//  Index Labels of categorical columns
  val categoricalIndexer: Array[PipelineStage] = categorical.map(
    column => new StringIndexer()
      .setInputCol(column)
      .setOutputCol(s"${column + indexSuffix}")
      .setHandleInvalid("keep")
      .asInstanceOf[PipelineStage]
  )

  println("####################\n\n####################\nSet Steps\n####################\n####################")
//  Unify above steps
  val steps: Array[PipelineStage] = numericalAssembler ++ numericalScaler ++ categoricalIndexer
  steps.foreach(x => println(x))
}
