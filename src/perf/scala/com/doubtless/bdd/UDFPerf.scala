import org.scalatest.funspec.FixtureAnyFunSpec
import com.doubtless.bdd._
import com.doubtless.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import scala.util.Random

class UDFPerf extends FixtureAnyFunSpec {
  type FixtureParam = SparkSession
  override protected def withFixture(test: OneArgTest) = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("UDFPerf")
      .getOrCreate()

    spark.udf.register("ProbCount", functions.udaf(ProbCountUDAF))
    spark.udf.register("ProbSum", functions.udaf(ProbSumUDAF))

    try test(spark)
    finally {
      spark.stop()
    }
  }

  describe("The ProbSum UDAF") {
   it("should scale exponentially") { spark =>
     import spark.implicits._

     val bdds = Array(
       BDD("1"),
       BDD("x=1"),
       BDD("(x=1|y=1)"),
       BDD("(x=1&y=1)|(z=5)"),
       BDD("(c=0&!b=0)"),
       BDD("x=1|(y=1 & x=2)"),
       BDD("(!(c=0|a=1)|!(b=2))"),
       BDD("(z=1)&!((x=1)&((y=1|y=2)&x=2))"),
       BDD("(x=8|x=2|x=4|x=3|x=9|(x=6|q=4&p=6)|x=7|x=1|x=5)"),
       BDD("(x=1&x=2|x=3&x=4|x=5&x=6|x=7&x=8|y=4)")
     )

     val n_groups = 1
     val group_size = 21
     val min_val = -100
     val max_val = 100

     val rows = 1 to n_groups flatMap { group =>
       1 to group_size map { _ =>
         (
           group,
           Random.nextDouble * (Math.abs(min_val) + Math.abs(
             max_val
           )) + min_val,
           bdds(Random.nextInt(bdds.size))
         )
       }
     }

     val t0 = System.currentTimeMillis()

     val res = rows
       .toDF("group", "value", "sentence")
       .groupBy("group")
       .agg(expr("ProbSum(value,sentence)").as("total"))
       .select(
         col("group"),
         explode(col("total"))
       )
       .withColumnsRenamed(
         Map(
           "key" -> "total",
           "value" -> "sentence"
         )
       )
       .select("group", "total", "sentence")
     res.collect()

     val t1 = System.currentTimeMillis()

     println(s"Row count: ${res.count()}")
     println(s"Elapsed time: ${t1 - t0}ms")
   }
  }

  describe("The ProbCount UDAF") {
    it("should scale exponentially") { spark =>
      import spark.implicits._

      val bdds = Array(
        BDD("1"),
        BDD("x=1"),
        BDD("(x=1|y=1)"),
        BDD("(x=1&y=1)|(z=5)"),
        BDD("(c=0&!b=0)"),
        BDD("x=1|(y=1 & x=2)"),
        BDD("(!(c=0|a=1)|!(b=2))"),
        BDD("(z=1)&!((x=1)&((y=1|y=2)&x=2))"),
        BDD("(x=8|x=2|x=4|x=3|x=9|(x=6|q=4&p=6)|x=7|x=1|x=5)"),
        BDD("(x=1&x=2|x=3&x=4|x=5&x=6|x=7&x=8|y=4)")
      )

      val n_groups = 4
      val group_size = 100

      val rows = 1 to n_groups flatMap { group =>
        1 to group_size map { _ => (group, bdds(Random.nextInt(bdds.size))) }
      }

      val t0 = System.currentTimeMillis()

      val res = rows
        .toDF("group", "sentence")
        .groupBy("group")
        .agg(expr("ProbCount(sentence)").as("total"))
        .select(
          col("group"),
          explode(col("total"))
        )
        .withColumnsRenamed(
          Map(
            "key" -> "total",
            "value" -> "sentence"
          )
        )
        .select("group", "total", "sentence")
      res.collect()

      val t1 = System.currentTimeMillis()

      println(s"Row count: ${res.count()}")
      println(s"Elapsed time: ${t1 - t0}ms")
    }
  }
}
