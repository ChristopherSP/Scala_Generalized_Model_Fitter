//Statistical methods to clean and transform data
//Pretty general

package com.autoML

import VariablesYAML._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._

object PreProcessing {
  //Get context created in main method to use spark implicits
  val sc: SparkContext = SparkContext.getOrCreate()
  val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

  import spark.implicits._

  def getColumnsByType(df: Dataset[_], Type: String): Array[String] = {
    df.schema
      .fields
      .filter(field => field.dataType.toString.equals(Type))
      .map(_.name.toString)
  }

  //Defines a vector assembler
  def vecAssembler (columns: Array[String], outputColumnName: String): VectorAssembler = {
    val assembler = new VectorAssembler().setInputCols(columns).setOutputCol(outputColumnName)
    assembler
  }

  //Dissamble a vector into several columns
  def vecDisassembler (df: DataFrame, columns: List[String], vecColName: String, suffix: String): DataFrame = {
    val getElement = udf((vector: org.apache.spark.ml.linalg.Vector, element: Int) => vector(element))

    var aux = df
    val m = columns.indices.toList

    m.foreach(idx =>
      aux = aux.withColumn(columns(idx) + suffix,
        getElement(
          col(vecColName),
          lit(idx)
        )
      )
    )
    val output = aux
    output
  }

  //Scale the numerical columns passed as a parameter
  def scale (df: DataFrame, columns: Array[String]): (StandardScalerModel, DataFrame) = {
    val dfFeaturesToScale = vecAssembler(columns, featuresName).transform(df)

    val scaler = new StandardScaler()
      .setInputCol(featuresName)
      .setOutputCol(scaledFeaturesName)
      .setWithStd(true)
      .setWithMean(true)

//    Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dfFeaturesToScale)

//    Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dfFeaturesToScale).drop(featuresName)

    val scaledColumns = vecDisassembler(scaledData, columns.toList, scaledFeaturesName, scaledSuffix)

    (scalerModel,scaledColumns)

  }

  //Remove global outliers, i.e., points that are at a great distance from the overall mean of the numerical
  // variables, more precisely, points that are at the 5% tail of the distance distribution
  def outlierRemover (df: DataFrame, columns: Array[String], idColName: String, sigLevel: Double): DataFrame = {
    val scaledData = scale(df, columns)._2
    val distanceName = "distanceToMean"

    val distance = df.join(scaledData.rdd
      .map(x => (x.getAs[String](idColName),Math.sqrt(
        Vectors.sqdist(
          x.getAs(scaledFeaturesName).asInstanceOf[org.apache.spark.ml.linalg.Vector]
          ,Vectors.zeros(columns.length).toDense))))
      .toDF(idColName,distanceName),Seq(idColName),"left")


    distance.createOrReplaceTempView("distTable")
    val percentiles = spark.sql("SELECT  PERCENTILE(" + distanceName + ", " + (sigLevel/2).toString + ") as " +
      "lowerBoundary, " +
       "PERCENTILE(" + distanceName + ", " + (1-sigLevel/2).toString + ") as upperBoundary FROM distTable").first()

    spark.catalog.dropTempView("distTable")

    val lowerBoundary = percentiles.getDouble(0)
    val upperBoundary = percentiles.getDouble(1)

    val distPercentile = distance.withColumn("lowerBoundary",lit(lowerBoundary))
      .withColumn("upperBoundary",lit(upperBoundary))

    distPercentile.filter(col(distanceName)>= $"lowerBoundary" && col(distanceName)<= $"upperBoundary")
      .drop(distanceName,"lowerBoundary","upperBoundary")
  }

  //Transform categorical variables into its numerical representation
  def stringIndexer (df: DataFrame, columns: Array[String]): DataFrame = {

    val indexTransformers: Array[org.apache.spark.ml.PipelineStage] = columns.map(
      column => new StringIndexer()
        .setInputCol(column)
        .setOutputCol(s"${column + indexSuffix}")
    )

    // Add the rest of your pipeline like VectorAssembler and algorithm
    val indexPipeline = new Pipeline().setStages(indexTransformers)
    val indexModel = indexPipeline.fit(df)
    val indexedDF = indexModel.transform(df)
    indexedDF

  /*  val categoricalIndexed = columns.flatMap(col => List(col + suffix))

    vecAssembler(indexedDF, categoricalIndexed, categoricalIndexedName)*/
  }

  def fillNumericMissingValues (df: DataFrame): DataFrame = {
    val numerical = df.schema
      .fields
      .filter(field => field.dataType.toString.equals("DoubleType"))
      .map(_.name.toString)

    val imputer = new Imputer()
      .setInputCols(numerical)
      .setOutputCols(numerical.map(column => s"${column + inputedSuffix}"))
      .setStrategy("median")

    imputer.fit(df).transform(df)
  }

  def fillStringMissingValues (df: DataFrame): DataFrame = {
    val dataTypeList = df.dtypes.toList

    val stringColumnList = dataTypeList.filter(kind =>
      kind._2.startsWith("Str"))
      .map(kind => kind._1)

    val frequencyList = stringColumnList.map(column =>
      (column,
        df.groupBy(column)
          .count()
          .sort(desc("count"))
          .take(1)(0)
          .get(0)
          .asInstanceOf[String]
      )
    )

    var aux = df

    frequencyList.foreach(categorical =>
      aux = aux.withColumn(
        categorical._1 + inputedSuffix,
        when(
          col(categorical._1).isNull, lit(categorical._2)
        ).otherwise(
          col(categorical._1)
        )
      )
    )

    val output = aux
    output
  }

  def fillMissingValues (df: DataFrame): DataFrame = {
    val dt1 = fillNumericMissingValues(df)
    val dt2 = fillStringMissingValues(dt1)

    dt2
  }
}
