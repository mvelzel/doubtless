package com.doubtless

import scala.util._
import com.doubtless.bdd._
import com.doubtless.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main extends App {
  case class Test(id: Int, bdd: BDD, group: Int)

  val spark = createSparkSession("SparkTest", isLocal = true)
  import spark.implicits._

  val groupVariables = 10
  var variableAlternatives = 10

  val inputDF = (0 until groupVariables flatMap { variable =>
    (0 until variableAlternatives map { alternative =>
      (0, BDD(s"g$variable=$alternative"))
    })
  }).toDF("group", "sentence")

  val test = inputDF
    .groupBy("group")
    .agg(expr("ProbCount(sentence)").as("count"))
    .select(
      col("group"),
      explode(col("count"))
    )
    .withColumnsRenamed(
      Map(
        "key" -> "count",
        "value" -> "sentence"
      )
    )
    .select("group", "sentence", "count")
    .orderBy(asc("group"), asc("count"))

  test.show()
  // val bdd = test.head().getAs[BDD]("sentence")

  // println(bdd.toExpr())
  // println("Running equivalence:")
  // println(bdd == BDD.False)
  // println(bdd.toDot())

  // val df = spark.read.json("data/offers_english.json")
  // df.printSchema()
  // df.show()
}
