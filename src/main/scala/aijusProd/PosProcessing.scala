package aijusProd

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object PosProcessing {
  val getElement: UserDefinedFunction = udf((element: org.apache.spark.ml.linalg.Vector, idx: Int) => element(idx))
}
