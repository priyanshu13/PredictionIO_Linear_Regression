package org.template.vanilla

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.controller.SanityCheck
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import grizzled.slf4j.Logger

case class DataSourceParams(
appName: String,
 evalK: Option[Int]
)extends Params

class ConsumptionEvent(
  val total_amount_withdrawn :  Double,
  val prevweek_mean:          Double,
  val trans_month:            Double, 
  val trans_year:       Double, 
  val trans_date_set:   Double,
  val weekday : Double,
  val festival_religion : Double,
  val working_day : Double,
  val holiday_sequence: Double
  
) extends Serializable

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

type ConsumptionEvents = RDD[ConsumptionEvent]

  @transient lazy val logger = Logger[this.type]

 //Read all events involving "point" type 
       

def readData(sc: SparkContext): ConsumptionEvents = { PEventStore.find(
      
      appName = dsp.appName,
      entityType = Some("Regression_atm"))(sc)
      // only keep entities with these required properties defined
      //required = Some(List("total_amount_withdrawn", "festival_religion", "working_day", "prevweek_mean", "trans_month", "weekday", "atm_name", "trans_year", "no", "holiday_sequence", "trans_date_set")))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { event =>
        try {
         new ConsumptionEvent(
	//Converting to Labeled Point as the LinearRegression Algorithm requires
          total_amount_withdrawn = event.properties.get[Double]("total_amount_withdrawn"),
          prevweek_mean = event.properties.get[Double]("prevweek_mean"),
	  trans_month = event.properties.get[Double]("trans_month"),
	  trans_year = event.properties.get[Double]("trans_year"), 
          trans_date_set = event.properties.get[Double]("trans_date_set"),
          weekday = event.properties.get[String]("weekday") match{
                                                          case "MONDAY" => 0
                                                          case "TUESDAY" => 1
                                                          case "WEDNESDAY" => 2
                                                          case "THURSDAY" => 3
                                                          case "FRIDAY" => 4
                                                          case "SATURDAY" => 5 
                                                          case "SUNDAY" => 6},
          festival_religion = event.properties.get[String]("festival_religion") match{
                                                          case "NH" => 0
                                                          case "H" => 1},
                                                           
          working_day = event.properties.get[String]("working_day") match{
                                                          case "W" => 0
                                                          case "H" => 1},
                                                          
          holiday_sequence = event.properties.get[String]("holiday_sequence") match{
                                                          case "HHH" => 0
                                                          case "HHW" => 1
                                                          case "HWH" => 2
                                                          case "WHW" => 3
                                                          case "HWW" => 4
                                                          case "WHH" => 5
                                                          case "WWW" => 6
                                                          case "WWH" => 7}
          )
        } catch {
          case e: Exception => {
            logger.error(s"Cannot convert ${event}. Exception: ${e}.")
            throw e
          }
        }
      }
   }
override
  def readTraining(sc: SparkContext): TrainingData = {
    val data: ConsumptionEvents = readData(sc)
    println("Data gone to data variable")
    new TrainingData(data)
  }

override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(dsp.evalK.nonEmpty, "DataSourceParams.evalK must not be None")

    val data: ConsumptionEvents = readData(sc)

    // K-fold splitting
    val evalK = dsp.evalK.get
    val indexedPoints: RDD[(ConsumptionEvent, Long)] = data.zipWithIndex()

    (0 until evalK).map { idx =>
      val trainingPoints = indexedPoints.filter(_._2 % evalK != idx).map(_._1)
      val testingPoints = indexedPoints.filter(_._2 % evalK == idx).map(_._1)

      (
        new TrainingData(trainingPoints),
        new EmptyEvaluationInfo(),
        testingPoints.map {
          p => (new Query(Array(p.prevweek_mean, p.trans_month, p.trans_year, p.trans_date_set, p.weekday, p.festival_religion, p.working_day, p.holiday_sequence)), new ActualResult(p.total_amount_withdrawn))
        }
        )
    }
  } 
}


class TrainingData(
  val data: RDD[ConsumptionEvent]
) extends Serializable with SanityCheck {

  override def sanityCheck(): Unit = {
    require(data.take(1).nonEmpty, s"data cannot be empty!")
  }
}
