package com.squareone.bankiq.Application.service.utility

import org.apache.spark.sql.SparkSession

  trait SparkServices

  class SparkService extends SparkServices {
  }


  object SparkService {

    def getSparkSession(): SparkSession = {

      val session = SparkSession.getActiveSession
      session match {
        case None => createSparkSession()
        case Some(_) => session.get
      }

    }

    def createSparkSession(): SparkSession = {
      SparkSession.builder().appName("ML").master("master").getOrCreate()
    }
  }
