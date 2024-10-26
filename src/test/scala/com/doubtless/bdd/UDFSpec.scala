import org.scalatest.funspec.FixtureAnyFunSpec
import org.apache.spark.sql.SparkSession
import com.doubtless.spark.createSparkSession
import com.doubtless.bdd.BDD
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.doubtless.spark._
import org.apache.spark.sql.functions

class UDFSpec extends FixtureAnyFunSpec with DatasetComparer {
  type FixtureParam = SparkSession
  override protected def withFixture(test: OneArgTest) = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("UDFSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.udf.register("ProbCount", functions.udaf(ProbCountUDAF))

    try test(spark)
    finally {
      spark.stop()
    }
  }

  describe("The ProbCount UDAF") {
    it("should correctly count over all possible worlds") { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, BDD("x=1")),
        (1, BDD("y=2")),
        (1, BDD("z=3")),
        (2, BDD("y=1")),
        (2, BDD("y=1")),
        (2, BDD("y=1")),
        (2, BDD("z=2"))
      ).toDF("group", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!y=2&!z=3"), 0),
        (1, BDD("(x=1&!y=2&!z=3)|(!x=1&y=2&!z=3)|(!x=1&!y=2&z=3)"), 1),
        (1, BDD("(x=1&y=2&!z=3)|(x=1&!y=2&z=3)|(!x=1&y=2&z=3)"), 2),
        (1, BDD("x=1&y=2&z=3"), 3),
        (2, BDD("!y=1&!z=2"), 0),
        (2, BDD("!y=1&z=2"), 1),
        (2, BDD("y=1&!z=2"), 3),
        (2, BDD("y=1&z=2"), 4)
      ).toDF("group", "sentence", "count")

      val actualDF = inputDF
        .groupBy("group")
        .agg(expr("ProbCount(sentence)").as("sum"))
        .select(
          col("group"),
          explode(col("sum"))
        )
        .withColumnsRenamed(
          Map(
            "key" -> "count",
            "value" -> "sentence"
          )
        )
        .select("group", "sentence", "count")

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

  it("should correctly leave out impossible worlds") { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, BDD("x=1")),
        (1, BDD("x=2")),
        (1, BDD("x=3")),
        (2, BDD("y=1")),
        (2, BDD("y=2"))
      ).toDF("group", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!x=3"), 0),
        (1, BDD("x=1|x=2|x=3"), 1),
        (2, BDD("!y=1&!y=2"), 0),
        (2, BDD("y=1|y=2"), 1),
      ).toDF("group", "sentence", "count")

      val actualDF = inputDF
        .groupBy("group")
        .agg(expr("ProbCount(sentence)").as("sum"))
        .select(
          col("group"),
          explode(col("sum"))
        )
        .withColumnsRenamed(
          Map(
            "key" -> "count",
            "value" -> "sentence"
          )
        )
        .select("group", "sentence", "count")

      assertSmallDatasetEquality(actualDF, expectedDF)
    }
  }
}
