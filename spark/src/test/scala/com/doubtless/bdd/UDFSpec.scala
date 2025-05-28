import org.scalatest.funspec.FixtureAnyFunSpec
import org.apache.spark.sql.SparkSession
import com.doubtless.spark.createSparkSession
import com.doubtless.bdd.BDD
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.doubtless.spark._
import org.apache.spark.sql.functions
import org.scalatest.BeforeAndAfterEach
import com.typesafe.config.ConfigFactory

class UDFSpec
    extends FixtureAnyFunSpec
    with DatasetComparer
    with BeforeAndAfterEach {
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
    spark.udf.register("ProbMin", functions.udaf(ProbMinUDAF))

    try test(spark)
    finally {
      spark.stop()
    }
  }

  val configPath = "com.doubtless.spark";
  var originalProperties: Map[String, String] = Map()

  override def beforeEach() = {
    originalProperties =
      sys.props.filter(tup => tup._1.startsWith(configPath)).toMap

    sys.props += s"$configPath.prob-sum.filter-on-finish" -> "true"
    sys.props += s"$configPath.prob-count.filter-on-finish" -> "true"
    sys.props += s"$configPath.prob-min.filter-on-finish" -> "true"

    ConfigFactory.invalidateCaches()
  }

  override def afterEach() = {
    originalProperties.foreach { case (k, v) =>
      sys.props.update(k, v)
    }

    val testProps =
      sys.props.filter(tup => tup._1.startsWith(configPath)).keySet
    testProps.diff(originalProperties.keySet).foreach(sys.props.remove)

    ConfigFactory.invalidateCaches()
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

    it(
      "should correctly small sum with multiple variables with multiple values"
    ) { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, 1.5, BDD("x=1")),
        (1, 2.0, BDD("x=2")),
        (1, 0.5, BDD("y=1")),
        (1, 1.5, BDD("y=2"))
      ).toDF("group", "num", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!y=1&!y=2"), 0.0),
        (1, BDD("(y=1&!x=1&!x=2)"), 0.5),
        (1, BDD("(x=1&!y=1&!y=2)|(y=2&!x=1&!x=2)"), 1.5),
        (1, BDD("(x=1&y=1)|(x=2&!y=1&!y=2)"), 2.0),
        (1, BDD("x=2&y=1"), 2.5),
        (1, BDD("x=1&y=2"), 3.0),
        (1, BDD("x=2&y=2"), 3.5)
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

    it("should correctly sum with multiple variables with multiple values") {
      spark =>
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
          (
            1,
            BDD(
              "(x=1&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=1&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            1.0
          ),
          (
            1,
            BDD(
              "(x=1&y=1)|(x=2&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=2&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            2.0
          ),
          (
            1,
            BDD(
              "(x=1&y=2)|(x=2&y=1)|(x=3&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=3&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            3.0
          ),
          (
            1,
            BDD(
              "(x=1&y=3)|(x=2&y=2)|(x=3&y=1)|(x=4&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=4&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            4.0
          ),
          (
            1,
            BDD(
              "(x=1&y=4)|(x=2&y=3)|(x=3&y=2)|(x=4&y=1)|(x=5&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=5&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            5.0
          ),
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
    // it("outputs the correct graph") { spark =>
    //  import spark.implicits._

    //  val inputDF = Seq(
    //    (1, BDD("x1=1")),
    //    (1, BDD("x2=1")),
    //    (1, BDD("x3=1")),
    //    (1, BDD("x4=1")),
    //  ).toDF("group", "sentence")

    //  val groupedDF = inputDF
    //    .groupBy("group")
    //    .agg(expr("ProbCount(sentence)").as("count"))
    //    .select(
    //      col("group"),
    //      explode(col("count"))
    //    )
    //    .withColumnsRenamed(
    //      Map(
    //        "key" -> "count",
    //        "value" -> "sentence"
    //      )
    //    )
    //    .filter(col("count") === 3)

    //  val resBDD = groupedDF.head().getAs[BDD]("sentence")
    //  assert(resBDD.toDot().replaceAll("""\s\s+""", " ").replace("\n", " ") == """digraph { labelloc="t"; label="bdd('((x4=1&((x3=1&((x2=1&!x1=1)|(!x2=1&x1=1)))|(!x3=1&(x2=1&x1=1))))|(!x4=1&(x3=1&(x2=1&x1=1))))')"; node [shape=square] 0 [label=<<b>0</b>>] node [shape=square] 1 [label=<<b>1</b>>] node [shape=circle] 2 [label=<<b>x1=1</b>>] edge [shape=rarrow style=dashed] 2 -> 1 edge [shape=rarrow style=bold] 2 -> 0 node [shape=circle] 3 [label=<<b>x1=1</b>>] edge [shape=rarrow style=dashed] 3 -> 0 edge [shape=rarrow style=bold] 3 -> 1 node [shape=circle] 4 [label=<<b>x2=1</b>>] edge [shape=rarrow style=dashed] 4 -> 3 edge [shape=rarrow style=bold] 4 -> 2 node [shape=circle] 5 [label=<<b>x2=1</b>>] edge [shape=rarrow style=dashed] 5 -> 0 edge [shape=rarrow style=bold] 5 -> 3 node [shape=circle] 6 [label=<<b>x3=1</b>>] edge [shape=rarrow style=dashed] 6 -> 5 edge [shape=rarrow style=bold] 6 -> 4 node [shape=circle] 7 [label=<<b>x3=1</b>>] edge [shape=rarrow style=dashed] 7 -> 0 edge [shape=rarrow style=bold] 7 -> 5 node [shape=circle] 8 [label=<<b>x4=1</b>>] edge [shape=rarrow style=dashed] 8 -> 7 edge [shape=rarrow style=bold] 8 -> 6 } """)
    // }

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
          posexplode(col("count"))
        )
        .withColumnsRenamed(
          Map(
            "pos" -> "count",
            "col" -> "sentence"
          )
        )
        .filter("sentence is not null")
        .select("group", "sentence", "count")
        .orderBy(asc("group"), asc("count"))

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

    it("should correctly count with multiple variables with multiple values") {
      spark =>
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
          (
            1,
            BDD(
              "(x=1&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=1&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            1
          ),
          (
            1,
            BDD(
              "(x=1&y=1)|(x=2&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=2&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            2
          ),
          (
            1,
            BDD(
              "(x=1&y=2)|(x=2&y=1)|(x=3&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=3&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            3
          ),
          (
            1,
            BDD(
              "(x=1&y=3)|(x=2&y=2)|(x=3&y=1)|(x=4&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=4&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            4
          ),
          (
            1,
            BDD(
              "(x=1&y=4)|(x=2&y=3)|(x=3&y=2)|(x=4&y=1)|(x=5&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=5&!x=1&!x=2&!x=3&!x=4&!x=5)"
            ),
            5
          ),
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
            posexplode(col("count"))
          )
          .withColumnsRenamed(
            Map(
              "pos" -> "count",
              "col" -> "sentence"
            )
          )
          .filter("sentence is not null")
          .select("group", "sentence", "count")
          .orderBy(asc("group"), asc("count"))

        assertSmallDatasetEquality(actualDF, expectedDF)
    }

    it(
      "should correctly small count with multiple variables with multiple values"
    ) { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, BDD("x=1")),
        (1, BDD("x=2")),
        (1, BDD("x=2")),
        (1, BDD("y=1")),
        (1, BDD("y=2")),
        (1, BDD("y=2"))
      ).toDF("group", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!y=1&!y=2"), 0),
        (1, BDD("(!x=1&!x=2&y=1)|(!y=1&!y=2&x=1)"), 1),
        (1, BDD("(x=1&y=1)|(x=2&!y=1&!y=2)|(y=2&!x=1&!x=2)"), 2),
        (1, BDD("(x=1&y=2)|(x=2&y=1)"), 3),
        (1, BDD("(x=2&y=2)"), 4)
      ).toDF("group", "sentence", "count")
        .orderBy(asc("group"), asc("count"))

      val actualDF = inputDF
        .groupBy("group")
        .agg(expr("ProbCount(sentence)").as("count"))
        .select(
          col("group"),
          posexplode(col("count"))
        )
        .withColumnsRenamed(
          Map(
            "pos" -> "count",
            "col" -> "sentence"
          )
        )
        .filter("sentence is not null")
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
          posexplode(col("count"))
        )
        .withColumnsRenamed(
          Map(
            "pos" -> "count",
            "col" -> "sentence"
          )
        )
        .filter("sentence is not null")
        .select("group", "sentence", "count")

      assertSmallDatasetEquality(actualDF, expectedDF)
    }
  }

  describe("The ProbMin UDAF") {
    it("should correctly min a value for all possible worlds") { spark =>
      import spark.implicits._

      val inputDF = Seq[(Integer, java.lang.Double, BDD)](
        (1, 1, BDD("x=1")),
        (1, -1, BDD("y=2")),
        (1, 2, BDD("z=3")),
        (1, 3, BDD("a=4")),
        (1, 1, BDD("x=1")),
        (1, 2, BDD("x=1")),
        (1, 3, BDD("x=1")),
        (1, -1, BDD("x=1")),
        (1, -2, BDD("x=1")),
        (1, 2, BDD("x=1")),
        (1, 3, BDD("x=1")),
        (1, 4, BDD("x=2")),
        (1, 1, BDD("x=2")),
        (1, 2, BDD("x=2")),
        (1, 2, BDD("x=2"))
      ).toDF("group", "num", "sentence")

      val expectedDF = Seq[(Integer, BDD, java.lang.Double)](
        (1, BDD("!y=2&!x=1&!x=2&!z=3&!a=4"), null),
        (1, BDD("x=1"), -2.0),
        (1, BDD("y=2&!x=1"), -1.0),
        (1, BDD("x=2&!x=1&!y=2"), 1.0),
        (1, BDD("z=3&!x=2&!x=1&!y=2"), 2.0),
        (1, BDD("a=4&!z=3&!x=2&!x=1&!y=2"), 3.0)
      ).toDF("group", "sentence", "min")
        .orderBy(asc("group"), asc("min"))

      val actualDF = inputDF
        .groupBy("group")
        .agg(expr("ProbMin(num,sentence)").as("total"))
        .select(
          col("group"),
          explode(col("total")).as("exploded")
        )
        .select(
          col("group"),
          col("exploded._2").as("sentence"),
          col("exploded._1").as("min")
        )
        .orderBy(asc("group"), asc("min"))

      assertSmallDatasetEquality(actualDF, expectedDF)
    }

    ignore(
      "should correctly small sum with multiple variables with multiple values"
    ) { spark =>
      import spark.implicits._

      val inputDF = Seq(
        (1, 1.5, BDD("x=1")),
        (1, 2.0, BDD("x=2")),
        (1, 0.5, BDD("y=1")),
        (1, 1.5, BDD("y=2"))
      ).toDF("group", "num", "sentence")

      val expectedDF = Seq(
        (1, BDD("!x=1&!x=2&!y=1&!y=2"), 0.0),
        (1, BDD("(y=1&!x=1&!x=2)"), 0.5),
        (1, BDD("(x=1&!y=1&!y=2)|(y=2&!x=1&!x=2)"), 1.5),
        (1, BDD("(x=1&y=1)|(x=2&!y=1&!y=2)"), 2.0),
        (1, BDD("x=2&y=1"), 2.5),
        (1, BDD("x=1&y=2"), 3.0),
        (1, BDD("x=2&y=2"), 3.5)
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

    ignore(
      "should correctly sum with multiple variables with multiple values"
    ) { spark =>
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
        (
          1,
          BDD(
            "(x=1&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=1&!x=1&!x=2&!x=3&!x=4&!x=5)"
          ),
          1.0
        ),
        (
          1,
          BDD(
            "(x=1&y=1)|(x=2&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=2&!x=1&!x=2&!x=3&!x=4&!x=5)"
          ),
          2.0
        ),
        (
          1,
          BDD(
            "(x=1&y=2)|(x=2&y=1)|(x=3&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=3&!x=1&!x=2&!x=3&!x=4&!x=5)"
          ),
          3.0
        ),
        (
          1,
          BDD(
            "(x=1&y=3)|(x=2&y=2)|(x=3&y=1)|(x=4&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=4&!x=1&!x=2&!x=3&!x=4&!x=5)"
          ),
          4.0
        ),
        (
          1,
          BDD(
            "(x=1&y=4)|(x=2&y=3)|(x=3&y=2)|(x=4&y=1)|(x=5&!y=1&!y=2&!y=3&!y=4&!y=5)|(y=5&!x=1&!x=2&!x=3&!x=4&!x=5)"
          ),
          5.0
        ),
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
}
