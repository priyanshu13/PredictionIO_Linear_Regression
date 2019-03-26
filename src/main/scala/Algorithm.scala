package org.template.vanilla

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
 

import org.apache.spark.SparkContext
import grizzled.slf4j.Logger




case class AlgorithmParams(
//Whether the model should train with an intercept
  iterations:        Int    = 10,
  regParam:          Double = 1.2,
  miniBatchFraction: Double = 0.5, 
  stepSize:          Double = 1e-11
      //setRegParam(0.3)
      //setElasticNetParam(0.8)
      //setMaxIter(100)
      //setTol(1E-6)
) extends Params


// extends P2LAlgorithm if Model contains RDD[]

class algo(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, LinearRegressionModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]
  
  def train(sc:SparkContext, df: PreparedData): LinearRegressionModel = {
    // MLLib Linear Regression cannot handle empty training data.
   
    //It is set to True only in the intercept field is set to 1
    //Right now, I am inputting this parameter as an integer, could be changed to String or Bool as necessary
     val lin = new LinearRegressionWithSGD() 
     lin.setIntercept(true)
     lin.optimizer
      .setNumIterations(ap.iterations)
      .setRegParam(ap.regParam)
      .setMiniBatchFraction(ap.miniBatchFraction)
      .setStepSize(ap.stepSize)
       lin.run(df.data)
 }
   
  
def predict(model: LinearRegressionModel, query: Query): PredictedResult = {
      
    //val weights: org.apache.spark.mllib.linalg.Vector = model.weights
    //val intercept = model.intercept
    //val weightsData: Array[Double] = weights.asInstanceOf[DenseVector].values
    val label : Double = model.predict(Vectors.dense(query.features))
    new PredictedResult(label)//(,intercept,weightsData)
  }
}



class Model(mod: LinearRegressionModel) extends Serializable {

  @transient lazy val logger = Logger[this.type]

  def predict(query: Query): Double = {
    val res = Vectors.dense(query.features)
    mod.predict(res)
  }
}


