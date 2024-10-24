package com.doubtless

import scala.util._
import com.doubtless.bdd._
import com.doubtless.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main extends App {
  case class Test(id: Int, bdd: BDD)

  val spark = createSparkSession("Spark test", isLocal = true)
  import spark.implicits._

  val test = Seq(
    Test(1, BDD("x=1")),
    Test(2, BDD("x=2")),
    Test(3, BDD("x=1&y=1")),
    Test(4, BDD("x=2&y=2")),
    Test(5, BDD("y=1"))
  )

  val testDF = test.toDF

  testDF.show()

  val test2 = Seq(
    ProbDict(
      RandVar("x", 1) -> 0.1,
      RandVar("x", 2) -> 0.9,
      RandVar("y", 1) -> 0.4,
      RandVar("y", 2) -> 0.6
    )
  )

  val test2DF = test2.toDF

  test2DF.show()
}
