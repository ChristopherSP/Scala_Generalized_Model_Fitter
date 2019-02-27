package com.semantix.aijusProd

//Import Project classes
import PreProcessing._
import VariablesYAML._
import PipeObjects._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
//import org.apache.spark.sql.DataFrame

//Import Spark packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

import java.io.File
import com.rockymadden.stringmetric.similarity._
import shapeless.syntax.std.tuple._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MainAijus {
  //  Main Method
  def main (arg: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //  Starts spark session
    val conf = new SparkConf().setAppName("aijusProd01")
    //        .setMaster("yarn")

    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession
      .builder()
      //        .appName("teste01")
      //        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") // replace with your hivemetastore service's thrift url
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    //      import spark.sql
    //  Sets case sensitive header on spark
    //      spark.sql("set spark.sql.caseSensitive=true")

    //  Reads data
    //      val rawData = spark.read.format(fileType)
    //        .option("header",fileHeader)
    //        .option("delimiter",fileDelimiter)
    //  //      .option("quote", fileQuotationMark)
    //        //.option("inferSchema",fileSchema)
    //        .load(inputFile)

    def changeSchema(df: Dataset[_], newSchema: StructType): DataFrame = {
      var aux: DataFrame = df.toDF

      for (idx <- newSchema.fields.indices.toList) {
        aux = aux.withColumn(
          newSchema.fieldNames(idx) + "Tmp",
          aux.col(newSchema.fieldNames(idx))
            .cast(newSchema.fields(idx).dataType)
        ).drop(newSchema.fieldNames(idx))
          .withColumnRenamed(newSchema.fieldNames(idx) + "Tmp",newSchema.fieldNames(idx))
      }
      aux
    }

    //      val allColumns = rawData.columns.toList
    //      val stringColumns = allColumns.diff(numericalOriginal)
    //      val dataSchema = StructType(
    //        stringColumns.map(x => StructField(x, StringType, true)) ++
    //          numericalOriginal.map(x => StructField(x, DoubleType, true))
    //      )

    val encerrados = spark.sql("SELECT * FROM testdb.s3processo")

    encerrados.printSchema()

    //      val encerrados = changeSchema(rawData, dataSchema)

    //      encerrados.printSchema()
    val originalColumns = encerrados.columns
    val outputColumns = List("pasta",outputMod2 + unscaledSuffix, probMod3, outputMod3 + converterSuffix,
      probMod4, outputMod4 + converterSuffix, outputMod5 + unscaledSuffix)

    val finalColumns = originalColumns.toList ++ outputColumns //:+
    //        (outputMod6 + unscaledSuffix)

    //  Gets numerical and categorical columns by Type
    //      val numerical = getColumnsByType(encerrados, "DoubleType").map(column => column + inputedSuffix)
    //      val categorical = getColumnsByType(encerrados, "StringType").map(column => column + inputedSuffix)

    val numerical = numericalOriginal.map(name => name + inputedSuffix).toArray
    val categorical = categoricalOriginal.map(name => name + inputedSuffix).toArray
    //  Creates preprocessing steps to integrate in pipeline. The steps consists in vectorize, scale and convert string
    // labels

    val preProcessing = new PreProcess((numerical :+ (independentVariableMod2 + inputedSuffix) :+
      (independentVariableMod5 + inputedSuffix)).distinct, (categorical :+
      (independentVariableMod3 + inputedSuffix) :+ (independentVariableMod4 + inputedSuffix)).distinct)
    //    val preProcessing = new PreProcess(numericalOriginal.toArray, categoricalOriginal.toArray)


    //  Each of the following procedurals creates a model step to be inserted into the pipeline and adds the output of
    // the model to the next through de list aditionalVariables
    var aditionalVariables: List[String]  = List()

    println("####################\n\n####################\nMain Mod2\n####################\n####################")
    val model2 = new ProcessDurationModel()

    aditionalVariables = aditionalVariables ++ List(outputMod2)

    println("####################\n\n####################\nMain Mod3\n####################\n####################")
    val model3 = new SentenceModel()
      .setAddtionalVariables(aditionalVariables)

    aditionalVariables = aditionalVariables ++ List(probMod3)
    println("####################\n\n####################\nMain Mod4\n####################\n####################")
    val model4 = new AgreementPropensityModel()
      .setAddtionalVariables(aditionalVariables)

    aditionalVariables = aditionalVariables ++ List(probMod4)
    println("####################\n\n####################\nMain Mod5\n####################\n####################")
    val model5 = new SentenceValueModel()
      .setAddtionalVariables(aditionalVariables)

    println("####################\n\n####################\nMain Mod6\n####################\n####################")
    val model6 = new AgreementValueModel()
      .setAddtionalVariables(aditionalVariables)
    //println("####################\n\n####################\nMain Mod7\n####################\n####################")
    //      val model7 = new ThesesAssociation()

    println("####################\n\n####################\nMain Pipe\n####################\n####################")
    //  Build the pipeline starting by filling NA values on the dataset. This is done by using the median of each numerical
    // column and the most frequence label of each categorical column
    val pipeline: Pipeline = new Pipeline().setStages(
      Array(new FillMissingValues()) ++
        preProcessing.steps ++
        Array(model2, model3, model4, model5, model6))

    println("####################\n\n####################\nMain Fit\n####################\n####################")
    //  Train all the chain in setted on the pipeline
    encerrados.repartition(300)
    encerrados.persist()
    encerrados.count()
    val teste = pipeline.fit(encerrados.filter($"status" === "Encerrado"))

    println("####################\n\n####################\nMain Transf\n####################\n####################")
    //  Apply fitted transformations to the dataset
    val output = teste.transform(encerrados)

    println("####################\n\n####################\nMain Output\n####################\n####################")
    output.persist()
    output.count()

    //  Display results
    val cleanOutput = output.select(outputColumns.head,outputColumns.tail: _*)
    cleanOutput.write.option("header", "false").csv(outputFile)


    //      val pedidos = spark.sql("SELECT * FROM testdb.items_pedidos")

    // Create and save to hive table
    // spark.sql("CREATE TABLE output_processos(key int, value string) STORED AS PARQUET")
    // cleanOutput.write.mode(SaveMode.Overwrite).saveAsTable("output_processos")

    //  Saves fitted pipeline into a spark structure in order to use it afterwards without the need to train the model again
    //  teste.write.overwrite.save("/home/christopher/Downloads/sample-pipeline")

    //  Reads saved pipeline making possible to predict new information without training a new model. NOT WORKING IN THE
    // MOMENT

    //    val pipeline = Pipeline.read.load("sample-pipeline")
  }
}
