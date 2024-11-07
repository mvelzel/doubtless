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
    spark.udf.register("ProbSum", functions.udaf(ProbSumUDAF))

    try test(spark)
    finally {
      spark.stop()
    }
  }

  describe("The ProbSum UDAF") {
    it("should correctly sum a value for all possible worlds") { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, 1, BDD("x=1")),
        (1, -1, BDD("y=2")),
        (1, 2, BDD("z=3")),
        (1, 3, BDD("a=4")),
        (2, 1, BDD("x=1")),
        (2, 2, BDD("x=1")),
        (2, 3, BDD("x=1")),
        (2, -1, BDD("x=1")),
        (2, -2, BDD("x=1")),
        (2, 2, BDD("x=1")),
        (2, 3, BDD("x=1")),
        (2, 4, BDD("x=2")),
        (2, 1, BDD("x=2")),
        (2, 2, BDD("x=2")),
        (2, 2, BDD("x=2"))
      ).toDF("group", "num", "sentence")

      val expectedDF = Seq(
        (1, BDD("y=2&!x=1&!z=3&!a=4"), -1.0),
        (1, BDD("(!x=1&!y=2&!z=3&!a=4)|(x=1&y=2&!z=3&!a=4)"), 0.0),
        (1, BDD("(x=1&!y=2&!z=3&!a=4)|(z=3&y=2&!x=1&!a=4)"), 1.0),
        (
          1,
          BDD("(z=3&!a=4&!y=2&!x=1)|(a=4&y=2&!z=3&!x=1)|(x=1&y=2&z=3&!a=4)"),
          2.0
        ),
        (
          1,
          BDD("(a=4&!z=3&!y=2&!x=1)|(a=4&y=2&x=1&!z=3)|(z=3&x=1&!y=2&!a=4)"),
          3.0
        ),
        (1, BDD("(a=4&x=1&!y=2&!z=3)|(a=4&z=3&y=2&!x=1)"), 4.0),
        (1, BDD("(a=4&z=3&!x=1&!y=2)|(a=4&z=3&y=2&x=1)"), 5.0),
        (1, BDD("x=1&z=3&a=4&!y=2"), 6.0),
        (2, BDD("!x=1&!x=2"), 0.0),
        (2, BDD("x=1"), 8.0),
        (2, BDD("x=2"), 9.0)
      ).toDF("group", "sentence", "total")
        .orderBy(asc("group"), asc("total"))

      val actualDF = inputDF
        .groupBy("group")
        .agg(expr("ProbSum(num,sentence)").as("total"))
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
        .select("group", "sentence", "total")
        .orderBy(asc("group"), asc("total"))

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

    it("should correctly small sum with multiple variables with multiple values") { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, 1.5, BDD("x=1")),
        (1, 2.0, BDD("x=2")),
        (1, 0.5, BDD("y=1")),
        (1, 1.5, BDD("y=2")),
      ).toDF("group", "num", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!y=1&!y=2"), 0.0),
        (1, BDD("(y=1&!x=1&!x=2)"), 0.5),
        (1, BDD("(x=1&!y=1&!y=2)|(y=2&!x=1&!x=2)"), 1.5),
        (1, BDD("(x=1&y=1)|(x=2&!y=1&!y=2)"), 2.0),
        (1, BDD("x=2&y=1"), 2.5),
        (1, BDD("x=1&y=2"), 3.0),
        (1, BDD("x=2&y=2"), 3.5),
      ).toDF("group", "sentence", "total")
        .orderBy(asc("group"), asc("total"))

      val actualDF = inputDF
        .groupBy("group")
        .agg(expr("ProbSum(num,sentence)").as("total"))
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
        .select("group", "sentence", "total")
        .orderBy(asc("group"), asc("total"))

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

    it("should correctly sum with multiple variables with multiple values") { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, 1, BDD("x=1")),
        (1, 2, BDD("x=2")),
        (1, 3, BDD("x=3")),
        (1, 4, BDD("x=4")),
        (1, 5, BDD("x=5")),
        (1, 1, BDD("y=1")),
        (1, 2, BDD("y=2")),
        (1, 3, BDD("y=3")),
        (1, 4, BDD("y=4")),
        (1, 5, BDD("y=5"))
      ).toDF("group", "num", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!x=3&!x=4&!x=5&!y=1&!y=2&!y=3&!y=4&!y=5"), 0.0),
        (1, BDD("(x=1&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=1&!x=1&!x=2&!x=3&!x=4&!x=5)"), 1.0),
        (1, BDD("(x=1&y=1)|(x=2&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=2&!x=1&!x=2&!x=3&!x=4&!x=5)"), 2.0),
        (1, BDD("(x=1&y=2)|(x=2&y=1)|(x=3&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=3&!x=1&!x=2&!x=3&!x=4&!x=5)"), 3.0),
        (1, BDD("(x=1&y=3)|(x=2&y=2)|(x=3&y=1)|(x=4&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=4&!x=1&!x=2&!x=3&!x=4&!x=5)"), 4.0),
        (1, BDD("(x=1&y=4)|(x=2&y=3)|(x=3&y=2)|(x=4&y=1)|(x=5&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=5&!x=1&!x=2&!x=3&!x=4&!x=5)"), 5.0),
        (1, BDD("(x=1&y=5)|(x=2&y=4)|(x=3&y=3)|(x=4&y=2)|(x=5&y=1)"), 6.0),
        (1, BDD("(x=2&y=5)|(x=3&y=4)|(x=4&y=3)|(x=5&y=2)"), 7.0),
        (1, BDD("(x=3&y=5)|(x=4&y=4)|(x=5&y=3)"), 8.0),
        (1, BDD("(x=4&y=5)|(x=5&y=4)"), 9.0),
        (1, BDD("(x=5&y=5)"), 10.0)
      ).toDF("group", "sentence", "total")
        .orderBy(asc("group"), asc("total"))

      val actualDF = inputDF
        .groupBy("group")
        .agg(expr("ProbSum(num,sentence)").as("total"))
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
        .select("group", "sentence", "total")
        .orderBy(asc("group"), asc("total"))

      assertSmallDatasetEquality(actualDF, expectedDF)
    }
  }

  describe("The ProbCount UDAF") {
    it("should correctly count for all possible worlds") { spark =>
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
        .orderBy(asc("group"), asc("count"))

      val actualDF = inputDF
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

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

    it("should correctly count with multiple variables with multiple values") { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, BDD("x=1")),
        (1, BDD("x=2")),
        (1, BDD("x=2")),
        (1, BDD("x=3")),
        (1, BDD("x=3")),
        (1, BDD("x=3")),
        (1, BDD("x=4")),
        (1, BDD("x=4")),
        (1, BDD("x=4")),
        (1, BDD("x=4")),
        (1, BDD("x=5")),
        (1, BDD("x=5")),
        (1, BDD("x=5")),
        (1, BDD("x=5")),
        (1, BDD("x=5")),
        (1, BDD("y=1")),
        (1, BDD("y=2")),
        (1, BDD("y=2")),
        (1, BDD("y=3")),
        (1, BDD("y=3")),
        (1, BDD("y=3")),
        (1, BDD("y=4")),
        (1, BDD("y=4")),
        (1, BDD("y=4")),
        (1, BDD("y=4")),
        (1, BDD("y=5")),
        (1, BDD("y=5")),
        (1, BDD("y=5")),
        (1, BDD("y=5")),
        (1, BDD("y=5"))
      ).toDF("group", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!x=3&!x=4&!x=5&!y=1&!y=2&!y=3&!y=4&!y=5"), 0),
        (1, BDD("(x=1&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=1&!x=1&!x=2&!x=3&!x=4&!x=5)"), 1),
        (1, BDD("(x=1&y=1)|(x=2&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=2&!x=1&!x=2&!x=3&!x=4&!x=5)"), 2),
        (1, BDD("(x=1&y=2)|(x=2&y=1)|(x=3&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=3&!x=1&!x=2&!x=3&!x=4&!x=5)"), 3),
        (1, BDD("(x=1&y=3)|(x=2&y=2)|(x=3&y=1)|(x=4&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=4&!x=1&!x=2&!x=3&!x=4&!x=5)"), 4),
        (1, BDD("(x=1&y=4)|(x=2&y=3)|(x=3&y=2)|(x=4&y=1)|(x=5&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=5&!x=1&!x=2&!x=3&!x=4&!x=5)"), 5),
        (1, BDD("(x=1&y=5)|(x=2&y=4)|(x=3&y=3)|(x=4&y=2)|(x=5&y=1)"), 6),
        (1, BDD("(x=2&y=5)|(x=3&y=4)|(x=4&y=3)|(x=5&y=2)"), 7),
        (1, BDD("(x=3&y=5)|(x=4&y=4)|(x=5&y=3)"), 8),
        (1, BDD("(x=4&y=5)|(x=5&y=4)"), 9),
        (1, BDD("(x=5&y=5)"), 10)
      ).toDF("group", "sentence", "count")
        .orderBy(asc("group"), asc("count"))

      val actualDF = inputDF
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

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

    it("should correctly small count with multiple variables with multiple values") { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, BDD("x=1")),
        (1, BDD("x=2")),
        (1, BDD("x=2")),
        (1, BDD("y=1")),
        (1, BDD("y=2")),
        (1, BDD("y=2")),
      ).toDF("group", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!y=1&!y=2"), 0),
        (1, BDD("(!x=1&!x=2&y=1)|(!y=1&!y=2&x=1)"), 1),
        (1, BDD("(x=1&y=1)|(x=2&!y=1&!y=2)|(y=2&!x=1&!x=2)"), 2),
        (1, BDD("(x=1&y=2)|(x=2&y=1)"), 3),
        (1, BDD("(x=2&y=2)"), 4),
      ).toDF("group", "sentence", "count")
        .orderBy(asc("group"), asc("count"))

      val actualDF = inputDF
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
        (2, BDD("y=1|y=2"), 1)
      ).toDF("group", "sentence", "count")

      val actualDF = inputDF
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

      assertSmallDatasetEquality(actualDF, expectedDF)
    }
  }
}
