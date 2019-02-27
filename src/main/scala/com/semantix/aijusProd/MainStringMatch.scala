//package aijusProd
//
////Import Spark packages
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.functions._
//
//import java.io.File
//import com.rockymadden.stringmetric.similarity._
//
//object MainStringMatch {
//  //  Main Method
//  def main (arg: Array[String]): Unit ={
//    //  Starts spark session
//    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
//
//    val conf = new SparkConf().setAppName("aijusProd01").setMaster("yarn")
//    val sc = SparkContext.getOrCreate(conf)
//    val spark = SparkSession
//      .builder()
//      .appName("teste01")
//      .config("spark.sql.warehouse.dir", warehouseLocation)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    //  Sets case sensitive header on spark
//    spark.sql("set spark.sql.caseSensitive=true")
//
//    import spark.implicits._
//    import spark.sql
//    //  Reads data
//    //    val cbo2002 = spark.read.format("csv")
//    //      .option("header","true")
//    //      .option("delimiter","\t")
//    //      .option("inferSchema","true")
//    //      .load("/home/christopher/Downloads/cboWide.csv")
//
//    val cbo2002 = sql("select * from testdb.cbo2002pdf")
//    cbo2002.show()
//
//    val family = cbo2002.select("titulos").map(r => r.getString(0)).collect.toList
//
//    //      def mostSimilar(input: String): String = {
//    //        val teste: String = "teste"
//    //        val similarityMetric: List[(String, Double)] =  family.map(ocupation =>
//    //          (ocupation, DiceSorensenMetric(1).compare(input, ocupation).getOrElse(Double).asInstanceOf[Double])
//    //        )com.rockymadden.stringmetric.
//    //
//    //        val output = similarityMetric.sortBy(- _._2)
//    //        output(0)._1
//    //      }
//
//
//    val mostSimilar = udf{(input: String) =>
//      val teste: String = "teste"
//      val similarityMetric: List[(String, Double)] =  family.map(ocupation =>
//        (ocupation, JaroMetric.compare(input, ocupation).getOrElse(Double).asInstanceOf[Double])
//      )
//
//      val output = similarityMetric.sortBy(- _._2)
//      output(0)
//    }
//
//    //      val enc = spark.read.format("csv")
//    //        .option("header","true")
//    //        .option("delimiter","\t")
//    //        .option("inferSchema","true")
//    //        .load("/home/christopher/Downloads/baseEnc.csv")
//    //          .na.fill("NULL")
//
//    val df = sql("select *, if(cargofuncao = '','NULL',cargofuncao) as cargofuncao2 from testdb.closedfileswm")
//    val enc = df.drop("cargofuncao").withColumnRenamed("cargofuncao2","cargofuncao")
//
//    enc.select("pasta","cargofuncao").show()
//
//    enc.withColumn("output", mostSimilar(col("cargofuncao")))
//      .withColumn("ocupation",$"output._1")
//      .withColumn("similarity",$"output._2")
//      .drop("output")
//        .select("cargofuncao","ocupation","similarity")
//          .distinct()
//        .orderBy(desc("similarity"))
//          .show(20000,false)
//
//    //        .coalesce(1)
//    //        .write
//    //        .option("header", "true")
//    //        .csv("/home/christopher/Downloads/baseEncOut.csv")
//
//    //      spark.stop()
//    //    sc.stop()
//    //    sys.exit()
//  }
//}
