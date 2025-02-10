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

  val inputDF = Seq(
    (0, BDD("g0=0")),
    (0, BDD("g0=1")),
    (0, BDD("g0=2")),
    (0, BDD("g0=3")),
    (0, BDD("g0=4")),
    (0, BDD("g1=0")),
    (0, BDD("g1=1")),
    (0, BDD("g1=2")),
    (0, BDD("g1=3")),
    (0, BDD("g1=4")),
    (0, BDD("g2=0")),
    (0, BDD("g2=1")),
    (0, BDD("g2=2")),
    (0, BDD("g2=3")),
    (0, BDD("g2=4")),
    (0, BDD("g3=0")),
    (0, BDD("g3=1")),
    (0, BDD("g3=2")),
    (0, BDD("g3=3")),
    (0, BDD("g3=4")),
    (0, BDD("g4=0")),
    (0, BDD("g4=1")),
    (0, BDD("g4=2")),
    (0, BDD("g4=3")),
    (0, BDD("g4=4")),
  ).toDF("group", "sentence")

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
  //val bdd = test.head().getAs[BDD]("sentence")

  //println(bdd.toExpr())
  //println("Running equivalence:")
  //println(bdd == BDD.False)
  //println(bdd.toDot())

  // val df = spark.read.json("data/offers_english.json")
  // df.printSchema()
  // df.show()
}
