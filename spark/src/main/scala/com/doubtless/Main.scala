package com.doubtless

import scala.util._
import com.doubtless.bdd._
import com.doubtless.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main extends App {
  case class Test(id: Int, bdd: BDD, group: Int)

  val spark = createSparkSession("SparkTest", isLocal=true)
  import spark.implicits._

  val test = spark.sql("select BDD('0')")
  test.show()

  spark.sql("SHOW FUNCTIONS").show(100, truncate = false)

  //val df = spark.read.json("data/offers_english.json")
  //df.printSchema()
  //df.show()
}
