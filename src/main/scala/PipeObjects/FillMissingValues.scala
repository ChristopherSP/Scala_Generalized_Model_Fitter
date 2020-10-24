package PipeObjects

//Import Project classes

//Import Spark packages
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, desc, lit, when}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import com.autoML.VariablesYAML._
//Fills NA values on the dataset. This is done by using the median of each numerical column and the most frequence
// label of each categorical column
class FillMissingValues(override val uid: String) extends Transformer with MyHasInputCol with MyHasOutputCol with
DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("fillmissingvalues"))

  def copy(extra: ParamMap): GetProb = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    val schema2 = schema.toArray.map(r => (r.name+inputedSuffix,r.dataType))
    var output : StructType = schema
    schema2.foreach(r => output = output.add(r._1,r._2,nullable = false))
    output
  }

//  Method for numerical columns
  def fillNumericMissingValues (df: DataFrame): DataFrame = {
//    Get numerical columns names by type
//    val numerical = df.schema
//      .fields
//      .filter(field => field.dataType.toString.equals("DoubleType"))
//      .map(_.name.toString)
    val numerical = (numericalOriginal.toArray :+ independentVariableMod2 :+ independentVariableMod5).distinct
//    Input median to NA values
    val imputer = new Imputer()
      .setInputCols(numerical)
      .setOutputCols(numerical.map(column => s"${column + inputedSuffix}"))
      .setStrategy("median")

    imputer.fit(df).transform(df)
  }

  //  Method for string columns
  def fillStringMissingValues (df: DataFrame): DataFrame = {
//    Get string columns names by type
    val dataTypeList = df.dtypes.toList

    val stringColumnList = (categoricalOriginal :+ independentVariableMod3 :+ independentVariableMod4).distinct
//    val stringColumnList = dataTypeList.filter(kind =>
//      kind._2.startsWith("Str"))
//      .map(kind => kind._1)

//    Calculates the frequency of each label for each column and gets the most frequent one
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

//    Input mode to NA values
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

  override def transform(df: Dataset[_]): DataFrame = {
    println("####################\n\n####################\nEnter " +
      "FillMissing\n####################\n####################")
    val dt1 = fillNumericMissingValues(df.toDF())
    val dt2 = fillStringMissingValues(dt1)
    println("####################\n\n####################\nExit " +
      "FillMissing\n####################\n####################")
    dt2
  }
}
