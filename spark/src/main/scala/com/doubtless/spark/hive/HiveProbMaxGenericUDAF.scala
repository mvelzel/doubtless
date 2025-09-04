package com.doubtless.spark.hive

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.ql.metadata.HiveException
import java.util.ArrayList
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.{
  AbstractAggregationBuffer,
  AggregationBuffer,
  Mode
}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.io.{DoubleWritable, BytesWritable, Text}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  PrimitiveObjectInspectorFactory,
  DoubleObjectInspector,
  BinaryObjectInspector,
  PrimitiveObjectInspectorUtils
}
import org.apache.hadoop.hive.serde2.objectinspector._;
import scala.jdk.CollectionConverters._
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import com.doubtless.spark.ProbMaxUDAF
import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  AbstractGenericUDAFResolver,
  GenericUDAFEvaluator,
  GenericUDAFParameterInfo
}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector

class ProbMaxAggBuffer extends AbstractAggregationBuffer {
  var resultList: List[(Option[Double], BDD, String)] = _
}

class HiveProbMaxGenericUDAFEvaluator extends GenericUDAFEvaluator {
  // Input ObjectInspectors
  private var inputMaxOI: DoubleObjectInspector = _
  private var inputBddOI: BinaryObjectInspector = _
  private var inputPruneMethodOI: StringObjectInspector = _

  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
  private var loi: ListObjectInspector = _;

  // Output ObjectInspectors
  private val interStructFieldNames: java.util.List[String] =
    java.util.List.of("max", "bdd", "pruneMethod")

  private val outputStructFieldNames: java.util.List[String] =
    java.util.List.of("max", "bdd")

  override def init(
      mode: Mode,
      parameters: Array[ObjectInspector]
  ): ObjectInspector = {
    super.init(mode, parameters)

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length != 3) {
        throw new UDFArgumentException(
          "prob_max requires 3 arguments (double, binary, string)"
        )
      }

      parameters(0) match {
        case oi: DoubleObjectInspector => inputMaxOI = oi
        case _ =>
          throw new UDFArgumentException("Argument 1 (number) must be a double")
      }

      parameters(1) match {
        case oi: BinaryObjectInspector => inputBddOI = oi
        case _ =>
          throw new UDFArgumentException("Argument 2 (bdd) must be binary")
      }

      parameters(2) match {
        case oi: StringObjectInspector => inputPruneMethodOI = oi
        case _ =>
          throw new UDFArgumentException(
            "Argument 3 (pruneMethod) must be a string"
          )
      }
    } else {
      parameters(0) match {
        case oi: ListObjectInspector => loi = oi
        case _ =>
          throw new UDFArgumentException("Intermediate result must be list")
      }
    }

    if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
      val interStructFieldOIs = new ArrayList[ObjectInspector]()
      interStructFieldOIs.add(
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
      )
      interStructFieldOIs.add(
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
      )
      interStructFieldOIs.add(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector
      )
      val interOutputStructOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(
          interStructFieldNames,
          interStructFieldOIs
        )

      return ObjectInspectorFactory.getStandardListObjectInspector(
        interOutputStructOI
      )
    } else {
      val outputStructFieldOIs = new ArrayList[ObjectInspector]()
      outputStructFieldOIs.add(
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
      )
      outputStructFieldOIs.add(
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
      )
      val finalOutputStructOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(
          outputStructFieldNames,
          outputStructFieldOIs
        )

      return ObjectInspectorFactory.getStandardListObjectInspector(
        finalOutputStructOI
      )
    }
  }

  override def getNewAggregationBuffer(): AggregationBuffer = {
    val res = new ProbMaxAggBuffer()
    reset(res)
    res
  }

  override def reset(agg: AggregationBuffer): Unit = {
    agg.asInstanceOf[ProbMaxAggBuffer].resultList = ProbMaxUDAF.zero
  }

  override def iterate(
      agg: AggregationBuffer,
      parameters: Array[Object]
  ): Unit = {
    if (
      parameters == null || parameters.length != 3 || parameters(
        0
      ) == null || parameters(1) == null || parameters(2) == null
    ) {
      return
    }

    val buffer = agg.asInstanceOf[ProbMaxAggBuffer]
    val maxVal =
      PrimitiveObjectInspectorUtils.getDouble(parameters(0), inputMaxOI);
    val bddVal = inputBddOI.getPrimitiveWritableObject(parameters(1))
    val pruneMethodVal =
      PrimitiveObjectInspectorUtils.getString(parameters(2), inputPruneMethodOI)

    val inputBdd = new BDD(bddVal.getBytes())

    buffer.resultList = ProbMaxUDAF.reduce(
      buffer.resultList,
      (Option(maxVal), inputBdd, pruneMethodVal)
    )
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial == null) return

    val buffer = agg.asInstanceOf[ProbMaxAggBuffer]
    val partialStructList =
      loi.getList(partial).asInstanceOf[java.util.List[ArrayList[AnyRef]]]

    val rightConvAgg = partialStructList.asScala
      .map(b => {
        val pruneMethod = b.get(2)
        (
          Option(b.get(0).asInstanceOf[DoubleWritable]).map(d => d.get()),
          new BDD(b.get(1).asInstanceOf[BytesWritable].getBytes()),
          if (pruneMethod == null) null
          else pruneMethod.asInstanceOf[Text].toString()
        )
      })
      .toList

    buffer.resultList = ProbMaxUDAF
      .merge(buffer.resultList, rightConvAgg)
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[ProbMaxAggBuffer]

    buffer.resultList
      .map({
        case (numOpt, bdd, pruneMethod) => {
          val number: DoubleWritable = numOpt match {
            case Some(num) => new DoubleWritable(num)
            case None      => null
          }

          val res = new ArrayList[AnyRef]()
          res.add(number)
          res.add(new BytesWritable(bdd.buffer))
          res.add(if (pruneMethod == null) null else new Text(pruneMethod))

          res
        }
      })
      .asJava
  }

  override def terminate(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[ProbMaxAggBuffer]
    if (buffer.resultList == null || buffer.resultList.isEmpty) {
      return null
    }

    ProbMaxUDAF
      .finish(buffer.resultList)
      .map({
        case (numOpt, bdd) => {
          val number: DoubleWritable = numOpt match {
            case Some(num) => new DoubleWritable(num)
            case None      => null
          }

          val res = new ArrayList[AnyRef]()
          res.add(number)
          res.add(new BytesWritable(bdd.buffer))

          res
        }
      })
      .asJava
  }
}

class HiveProbMaxGenericUDAF extends AbstractGenericUDAFResolver {
  @throws[HiveException]
  override def getEvaluator(
      parameters: Array[TypeInfo]
  ): GenericUDAFEvaluator = {
    if (parameters.length != 3) {
      throw new UDFArgumentException("prob_max requires 3 arguments")
    }
    // Add more specific TypeInfo checks for DOUBLE and BINARY if desired
    new HiveProbMaxGenericUDAFEvaluator()
  }

  @throws[HiveException]
  override def getEvaluator(
      info: GenericUDAFParameterInfo
  ): GenericUDAFEvaluator = {
    val parameters: Array[ObjectInspector] = info.getParameterObjectInspectors()
    if (parameters.length != 3) {
      throw new UDFArgumentException(
        "prob_max requires 3 arguments (from GenericUDAFParameterInfo)"
      )
    }
    // Can add specific ObjectInspector checks here
    new HiveProbMaxGenericUDAFEvaluator()
  }
}
