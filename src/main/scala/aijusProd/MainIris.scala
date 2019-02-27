//package aijusProd
//
////Import Project classes
//import aijusProd.PreProcessing._
//import aijusProd.Variables._
//import PipeObjects._
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.types.StructType
//
////Import Spark packages
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.sql.functions._
//
//import java.io.File
//import com.rockymadden.stringmetric.similarity._
//import shapeless.syntax.std.tuple._
//
//object MainIris {
////  Main Method
//    def main (arg: Array[String]): Unit ={
////  Starts spark session
//    val conf = new SparkConf().setAppName("aijusProd01").setMaster("local[*]")
//    val sc = SparkContext.getOrCreate(conf)
//    val spark = SparkSession
//      .builder()
//      .appName("teste01")
//      .getOrCreate()
//
//      import spark.implicits._
//  Sets case sensitive header on spark
//    spark.sql("set spark.sql.caseSensitive=true")
//
////  Reads data
//    val encerrados = spark.read.format("csv")
//      .option("header","true")
//      .option("delimiter",",")
//      .option("inferSchema","true")
//      .load(inputFile)
//
////  Gets numerical and categorical columns by Typecom.rockymadden.stringmetric.similarity._
//    val numerical = getColumnsByType(encerrados, "DoubleType").map(column => column + inputedSuffix)
//    val categorical = getColumnsByType(encerrados, "StringType").map(column => column + inputedSuffix)
//
////  Creates preprocessing steps to integrate in pipeline. The steps consists in vectorize, scale and convert string
//// labels
//    val preProcessing = new PreProcess(numerical, categorical)
//
////  Each of the following procedurals creates a model step to be inserted into the pipeline and adds the output of
//// the model to the next through de list aditionalVariables
//    var aditionalVariables: List[String]  = List()
//
//    val model2 = new ProcessDurationModel()
//
//    aditionalVariables = aditionalVariables ++ List(outputMod2)
//
//    val model3 = new SentenceModel()
//      .setAddtionalVariables(aditionalVariables)
//
//    aditionalVariables = aditionalVariables ++ List(probMod3)
//
//    val model4 = new AgreementPropensityModel()
//      .setAddtionalVariables(aditionalVariables)
//
//    aditionalVariables = aditionalVariables ++ List(probMod4)
//
//    val model5 = new SentenceValueModel()
//      .setAddtionalVariables(aditionalVariables)
//
////  Build the pipeline starting by filling NA values on the dataset. This is done by using the median of each numerical
//// column and the most frequence label of each categorical column
//    val pipeline: Pipeline = new Pipeline().setStages(
//      Array(new FillMissingValues()) ++
//      preProcessing.steps ++
//      Array(model2, model3, model4, model5))
//
////  Train all the chain in setted on the pipeline
//    val teste = pipeline.fit(encerrados)
//
////  Apply fitted transformations to the dataset
//    val output = teste.transform(encerrados)
////  Display results
//    output.show(150)
//
////  Saves fitted pipeline into a spark structure in order to use it afterwards without the need to train the model again
////  teste.write.overwrite.save("/home/christopher/Downloads/sample-pipeline")
//
////  Reads saved pipeline making possible to predict new information without training a new model. NOT WORKING IN THE
//    // MOMENT
//
////    val pipeline = Pipeline.read.load("sample-pipeline")
//
////    Stop spark sessions
//      spark.stop()
//    sc.stop()
//    sys.exit()
//  }
//}
