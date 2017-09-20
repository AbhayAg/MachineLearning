package com.squareone.bankiq.Application.entity

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.cloudera.sparkts.{BusinessDayFrequency, DateTimeIndex, TimeSeriesRDD}
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import com.squareone.bankiq.Application.service.business.Stocks.loadObservations
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by abhay on 20/9/17.
  */
object Entry extends App{

  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark-TS Ticker Example").setMaster("local")
    conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val tickerObs = loadObservations(sqlContext, "../data/ticker.tsv")

    // Create an daily DateTimeIndex over August and September 2015
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2015-08-03T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2015-09-22T00:00:00"), zone),
      new BusinessDayFrequency(1))

    // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs,
      "timestamp", "symbol", "price")

    // Cache it in memory
    tickerTsrdd.cache()

    // Count the number of series (number of symbols)
    println(tickerTsrdd.count())

    // Impute missing values using linear interpolation
    val filled = tickerTsrdd.fill("linear")

    // Compute return rates
    val returnRates = filled.returnRates()

    // Compute Durbin-Watson stats for each series
    val dwStats = returnRates.mapValues(TimeSeriesStatisticalTests.dwtest)

    println(dwStats.map(_.swap).min)
    println(dwStats.map(_.swap).max)
  }
}
