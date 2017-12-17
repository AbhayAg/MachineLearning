package com.cloudera.datascience

import org.apache.spark.sql.functions.first
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MatchData(
                      id_1: Int,
                      id_2: Int,
                      cmp_fname_c1: Option[Double],
                      cmp_fname_c2: Option[Double],
                      cmp_lname_c1: Option[Double],
                      cmp_lname_c2: Option[Double],
                      cmp_sex: Option[Int],
                      cmp_bd: Option[Int],
                      cmp_bm: Option[Int],
                      cmp_by: Option[Int],
                      cmp_plz: Option[Int],
                      is_match: Boolean
                    )

case class Score(value: Double) {
  def +(oi: Option[Int]) = {
    Score(value + oi.getOrElse(0))
  }
}

object MyApp extends App{

    val spark = SparkSession.builder().master("local").getOrCreate()
    //println("num lines: " + countLines(spark, args(0)))
    val parsed = spark.read.
      option("header", "true").
      option("nullValue", "?").
      option("inferSchema", "true").
      csv(args(0))

    parsed.cache()
    //developmentCode(parsed)
    productionCode(parsed)

  def productionCode(parsed:DataFrame) = {
    import spark.implicits._
    val matchData = parsed.as[MatchData]

    def scoreMatchData(md: MatchData): Double = {
      (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz +
        md.cmp_by + md.cmp_bd + md.cmp_bm).value
    }

    val scored = matchData.map { md =>
      (scoreMatchData(md), md.is_match)
    }.toDF("score", "is_match")

    def crossTabs(scored: DataFrame, t: Double): DataFrame = {
      scored.
        selectExpr(s"score >= $t as above", "is_match").
        groupBy("above").
        pivot("is_match", Seq("true", "false")).
        count()
    }
    crossTabs(scored, 5.0).show()
    crossTabs(scored, 4.5).show()
    crossTabs(scored, 4.0).show()
    crossTabs(scored, 3.0).show()
    crossTabs(scored, 2.0).show()
  }

  def developmentCode(parsed:DataFrame) = {
    import spark.implicits._
    parsed.groupBy("is_match").count().orderBy($"count".desc).show()
    //parsed.createOrReplaceTempView("linkage")
    val summary = parsed.describe()

    val matches = parsed.where("is_match = true")
    val matchSummary = matches.describe()
    val misses = parsed.filter($"is_match" === false)
    val missSummary = misses.describe()

    val matchSummaryT = pivotSummary(matchSummary)
    val missSummaryT = pivotSummary(missSummary)
    matchSummaryT.show()

    matchSummaryT.createOrReplaceTempView("match_desc")
    missSummaryT.createOrReplaceTempView("miss_desc")
    spark.sql("""
      SELECT a.field, a.count + b.count total, a.mean - b.mean delta
      FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
      WHERE a.field NOT IN ("id_1", "id_2")
      ORDER BY delta DESC, total DESC
      """).show()
  }

  def pivotSummary(desc: DataFrame): DataFrame = {
    val schema = desc.schema
    import desc.sparkSession.implicits._
    val lf = desc.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => {
        (metric, schema(i).name, row.getString(i).toDouble)
      })
    }).toDF("metric", "field", "value")
    lf.groupBy("field").
      pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))
  }
}