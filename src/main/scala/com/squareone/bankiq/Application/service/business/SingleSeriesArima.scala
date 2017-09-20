package com.squareone.bankiq.Application.service.business

/**
  * Created by abhay on 20/9/17.
  */
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors

/**
  * An example showcasing the use of ARIMA in a non-distributed context.
  */
object SingleSeriesARIMA {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.
    val lines = scala.io.Source.fromFile("src/main/resources/DataFile/ArimaDataset.csv").getLines()
    val ts = Vectors.dense(lines.map(_.toDouble).toArray)
    val arimaModel = ARIMA.fitModel(1, 0, 1, ts)
    println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 20)
    println("forecast of next 20 observations: " + forecast.toArray.mkString(","))
  }
}
