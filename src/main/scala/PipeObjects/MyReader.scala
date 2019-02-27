package PipeObjects

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{MLReadable, MLReader}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{compact, parse, render}

//Somewhat needed to read the pipeline models. Require first investigation and study
//DOES NOT WORK
class MyReader[T] extends MLReader[T] {

  override def load(path: String): T = {
    val metadata = MyReader.loadMetadata(path, sc)
    val cls = Class.forName(metadata.className)
    val instance =
      cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
    MyReader.getAndSetParams(instance, metadata)
    instance.asInstanceOf[T]
  }
}

private object MyReader {

  /**
    * All info from metadata file.
    *
    * @param params  paramMap, as a `JValue`
    * @param metadata  All metadata, including the other fields
    * @param metadataJson  Full metadata file String (for debugging)
    */
  case class Metadata(
                       className: String,
                       uid: String,
                       timestamp: Long,
                       sparkVersion: String,
                       params: JValue,
                       metadata: JValue,
                       metadataJson: String) {

    /**
      * Get the JSON value of the [[org.apache.spark.ml.param.Param]] of the given name.
      * This can be useful for getting a Param value before an instance of `Params`
      * is available.
      */
    def getParamValue(paramName: String): JValue = {
      implicit val format = DefaultFormats
      params match {
        case JObject(pairs) =>
          val values = pairs.filter { case (pName, jsonValue) =>
            pName == paramName
          }.map(_._2)
          assert(values.length == 1, s"Expected one instance of Param '$paramName' but found" +
            s" ${values.length} in JSON Params: " + pairs.map(_.toString).mkString(", "))
          values.head
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot recognize JSON metadata: $metadataJson.")
      }
    }
  }

  /**
    * Load metadata saved using [[DefaultParamsWriter.saveMetadata()]]
    *
    * @param expectedClassName  If non empty, this is checked against the loaded metadata.
    * @throws IllegalArgumentException if expectedClassName is specified and does not match metadata
    */
  def loadMetadata(path: String, sc: SparkContext, expectedClassName: String = ""): Metadata = {
    val metadataPath = new Path(path, "metadata").toString
    val metadataStr = sc.textFile(metadataPath, 1).first()
    parseMetadata(metadataStr, expectedClassName)
  }

  /**
    * Parse metadata JSON string produced by [[DefaultParamsWriter.getMetadataToSave()]].
    * This is a helper function for [[loadMetadata()]].
    *
    * @param metadataStr  JSON string of metadata
    * @param expectedClassName  If non empty, this is checked against the loaded metadata.
    * @throws IllegalArgumentException if expectedClassName is specified and does not match metadata
    */
  def parseMetadata(metadataStr: String, expectedClassName: String = ""): Metadata = {
    val metadata = parse(metadataStr)

    implicit val format = DefaultFormats
    val className = (metadata \ "class").extract[String]
    val uid = (metadata \ "uid").extract[String]
    val timestamp = (metadata \ "timestamp").extract[Long]
    val sparkVersion = (metadata \ "sparkVersion").extract[String]
    val params = metadata \ "paramMap"
    if (expectedClassName.nonEmpty) {
      require(className == expectedClassName, s"Error loading metadata: Expected class name" +
        s" $expectedClassName but found class name $className")
    }

    Metadata(className, uid, timestamp, sparkVersion, params, metadata, metadataStr)
  }

  /**
    * Extract Params from metadata, and set them in the instance.
    * This works if all Params implement [[org.apache.spark.ml.param.Param.jsonDecode()]].
    * TODO: Move to [[Metadata]] method
    */
  def getAndSetParams(instance: Params, metadata: Metadata): Unit = {
    implicit val format = DefaultFormats
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach { case (paramName, jsonValue) =>
          val param = instance.getParam(paramName)
          val value = param.jsonDecode(compact(render(jsonValue)))
          instance.set(param, value)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }

  /**
    * Load a `Params` instance from the given path, and return it.
    * This assumes the instance implements [[MLReadable]].
    */
  def loadParamsInstance[T](path: String, sc: SparkContext): T = {
    val metadata = MyReader.loadMetadata(path, sc)
    val cls = Class.forName(metadata.className)
    cls.getMethod("read").invoke(null).asInstanceOf[MLReader[T]].load(path)
  }
}