package PipeObjects

import org.apache.spark.ml.param.{Param, Params}

//Somewhat needed to save the pipeline models. Require first investigation and study
trait MyHasOutputCol extends Params {
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  def setOutputCol(value: String): this.type = set(outputCol, value)

  final def getOutputCol: String = $(outputCol)
}
