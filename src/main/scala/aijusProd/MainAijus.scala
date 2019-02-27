/*
// Author: Christopher Silva de Padua
// Framework: Spark Scala
// Purpose: To develop AI models for Semantix's product Aijus

package aijusProd

//Import Project classes
import aijusProd.PreProcessing._
import aijusProd.Preparation._
import Models.Model
import aijusProd.ClassificationModel._
import aijusProd.RF

//Import Spark packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MainAijus {
  def main (arg: Array[String]): Unit ={

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf().setAppName("aijusProd01").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession
      .builder()
      .appName("aijusProd01")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sqlContext


    spark.sql("set spark.sql.caseSensitive=true")

    val idCol = List("pasta")
    val tempo = List("tempo_processo")
    val valores = List("valor_da_causa", "valor_oca_correcao", "valor_oca_juros", "valor_provisao_wm_possivel",
      "valor_provisao_wm_remota")
    val oca = List("valor_provavel_oca")
    val ano = List("ano")
    //val partes = List("advogado_interno_walmart", "responsavel_do_escritorio", "parte_adversa", "escritorio")
    val partes = List("advogado_interno_walmart", "responsavel_do_escritorio", "escritorio")
    val cliente = List("cargofuncao", "n_unidade", "formato", "bandeira")
    val processo = List("objetotese", "instancia", "comarca", "uf", "tipo_de_acao", "tipo_de_contingencia",
      "regional")

    val allColumns: List[String] = idCol ++ tempo ++ partes ++ valores ++ oca ++ cliente ++ processo ++ ano
    val numerical: List[String] = tempo ++ valores ++ oca
    val categorical: List[String] = allColumns.diff(numerical ++ idCol)

    //    val numerical = encerrados.schema
    //      .fields
    //      .filter(x => x.dataType.toString.equals("DoubleType"))
    //      .map(_.name.toString)

    //Read and ETL data
    val (ativosPrep,encerradosPrep) = prepData(false)

    //Fill null with string NA, otherwise methods will fail
    val (ativos,encerrados) = (ativosPrep.na.fill("NA"),encerradosPrep.na.fill("NA"))


    //Other way is to eliminate null values, but thinking in production it's better to have a NA level
    /*val (ativos, encerrados) = (ativosPrep.select(allColumns.head,allColumns.tail:_*).na.drop,
      encerradosPrep.select(allColumns.head,allColumns.tail:_*).na.drop)*/

    //Remove overall outliers
    val encerradosOutlier = outlierRemover(encerrados, numerical.toArray, "pasta", 0.05)

    //Scale numerical data
    val (scaledModel, encerradosScaled) = scale(encerradosOutlier, numerical.toArray)

    val encerradoIndexed = stringIndexer(encerradosScaled, categorical.toArray)

    //Calls model3 with random forest classifier
    val modelo3 = ClassificationModel.getMethod(method = "RF", isClassification = true)
      .setDF(
        encerradoIndexed.filter($"label" =!= "acordo")
      )
      .setCategoricalVariables(
        (cliente ++ processo ++ ano ++ tempo).flatMap(col => List(col + "_index"))
      )
      .setNumericalVariables(oca)
      .setIndependentVariable("label")
      .setConstants()

    //Calls model3 with random forest classifier
    val modelo4 = ClassificationModel.getMethod(method = "RF", isClassification = true)
      .setDF(
        encerradoIndexed.filter($"label" =!= "improcedente")
      )
      .setCategoricalVariables(
        (cliente ++ processo ++ ano ++ tempo).flatMap(col => List(col + "_index"))
      )
      .setNumericalVariables(oca)
      .setIndependentVariable("label")
      .setConstants()

    //Prints parameters
    modelo3.getDF.show
    modelo3.getDependentVariables.foreach(println(_))
    println(modelo3.getIndependentVariables)
    modelo3.getHiperparameters.productIterator.foreach(println)

    println("Treino")

    val trainedModel3 = modelo3.train()

    println("Teste")
    val teste = trainedModel3.transform(modelo3.testSet)
    teste.show()

    println("Performance")
    modelo3.performance(trainedModel3, teste)

    ///////////////////////////////////////////////////////////////////////////////
    //               REMOVE THIS LINES WHEN CREATING THE JAR PACKAGE
    ///////////////////////////////////////////////////////////////////////////////
    spark.stop()
    sc.stop()
  }
}
*/
