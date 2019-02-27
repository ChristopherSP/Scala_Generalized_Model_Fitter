package PipeObjects

import org.apache.spark.ml.param.{Param, Params}

//Somewhat needed to save the pipeline models. Require first investigation and study
trait MyHasInputCol extends Params{
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  def setInputCol(value: String): this.type = set(inputCol, value)

  final def getInputCol: String = $(inputCol)
}
