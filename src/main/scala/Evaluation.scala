package org.template.vanilla

import org.apache.predictionio.controller.AverageMetric
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EngineParams
import org.apache.predictionio.controller.EngineParamsGenerator
import org.apache.predictionio.controller.Evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import math.{pow, sqrt}

case class RMSEMetric()
  extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {

  override
  def calculate(sc: SparkContext,
                evalDataSet: Seq[(EmptyEvaluationInfo,
                  RDD[(Query, PredictedResult, ActualResult)])]): Double = {
    sqrt(super.calculate(sc, evalDataSet))
  }

  def calculate(query: Query, predicted: PredictedResult, actual: ActualResult): Double =
    pow(predicted.label - actual.label, 2)

  override
  def compare(r0: Double, r1: Double): scala.Int = {
    -1 * super.compare(r0, r1)
  }
}

object RMSEEvaluation extends Evaluation {
  engineMetric = (VanillaEngine(), new RMSEMetric())
}

object EngineParamsList extends EngineParamsGenerator {

  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appName = "L_R_S", evalK = Some(5)))

  engineParamsList = Seq(
    baseEP.copy(
      algorithmParamsList = Seq(
        ("algo", AlgorithmParams(iterations = 10, stepSize = 1e-11, regParam = 1.2, miniBatchFraction = 0.5))
      )),
    baseEP.copy(
      algorithmParamsList = Seq(
        ("algo", AlgorithmParams(iterations = 10, stepSize = 1e-11, regParam = 0.1, miniBatchFraction = 0.1))
      ))
  )
}

