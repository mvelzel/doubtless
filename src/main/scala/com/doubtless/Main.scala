package com.doubtless

import scala.util._
import com.doubtless.bdd._
import com.doubtless.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main extends App {
  case class Test(id: Int, bdd: BDD)

  val spark = createSparkSession("Spark test", isLocal=true)
  import spark.implicits._

  val test = Seq(
    Test(1, BDD("x=1")),
    Test(2, BDD("x=2")),
    Test(3, BDD("x=1&x=2"))
  )

  val testDF = test.toDF

  testDF.show()
}
