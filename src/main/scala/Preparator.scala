package org.template.vanilla

import org.apache.predictionio.controller.PPreparator
import org.apache.predictionio.controller.SanityCheck

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

class PreparedData(
  val data: RDD[LabeledPoint]
) extends Serializable with SanityCheck {

  override def sanityCheck(): Unit = {
    require(data.take(1).nonEmpty, s"data cannot be empty!")
  }
}

class Preparator extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {

    val data = trainingData.data map {
      ev => LabeledPoint(ev.total_amount_withdrawn,
        Vectors.dense(Array(ev.prevweek_mean, ev.trans_month, ev.trans_year, ev.trans_date_set, ev.weekday, ev.festival_religion, ev.working_day, ev.holiday_sequence)))
    } cache()
    new PreparedData(data)
  }
}




