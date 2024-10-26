package com.doubtless

import scala.util._
import com.doubtless.bdd._
import com.doubtless.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main extends App {
  case class Test(id: Int, bdd: BDD, group: Int)

  val spark = createSparkSession("Spark test", isLocal = true)
  import spark.implicits._

  val test = Seq(
    Test(1, BDD("x=1"), 1),
    Test(2, BDD("x=2"), 1),
    Test(3, BDD("x=1&y=1"), 2),
    Test(4, BDD("x=2&y=2"), 2),
    Test(5, BDD("y=1"), 2)
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

  testDF
    .groupBy("group")
    .agg(
      expr("BDDAggOr(bdd)").as("OrAgg"),
      expr("BDDAggAnd(bdd)").as("AndAgg")
    )
    .show()

  val testCount = Seq(
    Test(1, BDD("x=1"), 1),
    Test(2, BDD("y=2"), 1),
    Test(3, BDD("x=1&y=1"), 2),
    Test(4, BDD("x=2&y=2"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(6, BDD("w=2&z=4"), 2),
    Test(6, BDD("w=2&z=4"), 2),
    Test(6, BDD("w=2&z=4"), 2),
    Test(6, BDD("w=2&z=4"), 2),
    Test(7, BDD("w=3"), 2),
    Test(8, BDD("z=1"), 2),
    Test(9, BDD("x=3"), 2),
    Test(9, BDD("x=3"), 2),
    Test(9, BDD("x=3"), 2),
    Test(10, BDD("y=1"), 2),
    Test(11, BDD("y=2"), 2),
    Test(12, BDD("y=4"), 2),
    Test(13, BDD("y=1"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(15, BDD("y=1"), 2),
    Test(16, BDD("y=1&z=1"), 2),
    Test(16, BDD("y=1&z=1"), 2),
    Test(16, BDD("y=1&z=1"), 2),
    Test(16, BDD("y=1&z=1"), 2),
    Test(17, BDD("y=3"), 2),
    Test(18, BDD("y=2"), 2),
    Test(19, BDD("z=1"), 2)
  )

  val t0 = System.currentTimeMillis()

  testCount.toDF
    .groupBy("group")
    .agg(expr("ProbCount(bdd)").as("count"))
    .select(
      col("group"),
      explode(col("count"))
    )
    .show(40)

  val t1 = System.currentTimeMillis()

  println(s"Elapsed time: ${t1 - t0}ms")

  val testSum = Seq(
    Test(1, BDD("x=1"), 1),
    Test(2, BDD("y=1"), 1),
    Test(3, BDD("z=1"), 1),
    Test(4, BDD("x=2&y=2"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(5, BDD("y=1|z=1&!x=4"), 2),
    Test(6, BDD("w=2&z=4"), 2),
    Test(6, BDD("w=2&z=4"), 2),
    Test(6, BDD("w=2&z=4"), 2),
    Test(7, BDD("w=3"), 2),
    Test(8, BDD("z=1"), 2),
    Test(9, BDD("x=3"), 2),
    Test(9, BDD("x=3"), 2),
    Test(9, BDD("x=3"), 2),
    Test(9, BDD("x=3"), 2),
    Test(12, BDD("y=4"), 2),
    Test(13, BDD("y=1"), 2),
    Test(13, BDD("y=1"), 2),
    Test(13, BDD("y=1"), 2),
    Test(13, BDD("y=1"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(14, BDD("y=1&x=2|y=2"), 2),
    Test(15, BDD("y=1"), 2),
    Test(16, BDD("y=1&z=1"), 2),
    Test(16, BDD("y=1&z=1"), 2),
    Test(17, BDD("y=3"), 2),
    Test(18, BDD("y=2"), 2),
    Test(19, BDD("z=1"), 2)
  )

  val t2 = System.currentTimeMillis()

  testSum.toDF
    .groupBy("group")
    .agg(expr("ProbSum(id,bdd)").as("sum"))
    .select(
      col("group"),
      explode(col("sum"))
    )
    .show(100)

  val t3 = System.currentTimeMillis()

  println(s"Elapsed time: ${t3 - t2}ms")
}
