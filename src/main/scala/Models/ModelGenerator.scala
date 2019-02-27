package Models

//Import Project classes
import Models.Classification._
import Models.Regression._
import aijusProd.Variables._

//Factory Pattern object that calls models
object ModelGenerator{

//  Constructor for classes that match a given parameter
  def getMethod (method: String): Model = {
    method match {
      case `rfClassifier` => new RFClassifier()
      case `rfRegression` => new RFRegression()
      case `gbmClassifier` => new GBMClassifier()
      case `gbmRegression` => new GBMRegression()
      case `glm` => new GLM()
      case `logistic` => new Logistic()
      case `svm` => new SVM()
      case `bayes` => new Bayes()
      case _ => null
    }
  }

}
