package com.squareone.bankiq.Application.service.business


import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object Stocks {

  def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0,
        ZoneId.systemDefault())
      val symbol = tokens(3)
      val price = tokens(5).toDouble
      Row(Timestamp.from(dt.toInstant), symbol, price)
    }
    val fields = Seq(
      StructField("timestamp", TimestampType, true),
      StructField("symbol", StringType, true),
      StructField("price", DoubleType, true)
    )
    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }
}
