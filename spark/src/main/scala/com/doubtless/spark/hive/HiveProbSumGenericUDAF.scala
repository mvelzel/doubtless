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
import org.apache.hadoop.io.{DoubleWritable, BytesWritable}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  PrimitiveObjectInspectorFactory,
  DoubleObjectInspector,
  BinaryObjectInspector,
  PrimitiveObjectInspectorUtils
}
import org.apache.hadoop.hive.serde2.objectinspector._;
import scala.jdk.CollectionConverters._
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import com.doubtless.spark.ProbSumUDAF
import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  AbstractGenericUDAFResolver,
  GenericUDAFEvaluator,
  GenericUDAFParameterInfo
}

class ProbSumAggBuffer extends AbstractAggregationBuffer {
  var resultList: List[(Double, BDD)] = _
}

class HiveProbSumGenericUDAFEvaluator extends GenericUDAFEvaluator {
  // Input ObjectInspectors
  private var inputSumOI: DoubleObjectInspector = _
  private var inputBddOI: BinaryObjectInspector = _

  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
  private var loi: ListObjectInspector = _;

  // Output ObjectInspectors
  private val structFieldNames: java.util.List[String] =
    java.util.List.of("sum", "bdd")

  override def init(
      mode: Mode,
      parameters: Array[ObjectInspector]
  ): ObjectInspector = {
    super.init(mode, parameters)

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length != 2) {
        throw new UDFArgumentException(
          "prob_sum requires 2 arguments (double, binary)"
        )
      }

      parameters(0) match {
        case oi: DoubleObjectInspector => inputSumOI = oi
        case _ =>
          throw new UDFArgumentException("Argument 1 (number) must be a double")
      }

      parameters(1) match {
        case oi: BinaryObjectInspector => inputBddOI = oi
        case _ =>
          throw new UDFArgumentException("Argument 2 (bdd) must be binary")
      }
    } else {
      parameters(0) match {
        case oi: ListObjectInspector => loi = oi
        case _ =>
          throw new UDFArgumentException("Intermediate result must be list")
      }
    }

    val outputStructFieldOIs = new ArrayList[ObjectInspector]()
    outputStructFieldOIs.add(
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
    )
    outputStructFieldOIs.add(
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
    )
    val finalOutputStructOI = ObjectInspectorFactory
      .getStandardStructObjectInspector(structFieldNames, outputStructFieldOIs)

    return ObjectInspectorFactory.getStandardListObjectInspector(
      finalOutputStructOI
    )
  }

  override def getNewAggregationBuffer(): AggregationBuffer = {
    val res = new ProbSumAggBuffer()
    reset(res)
    res
  }

  override def reset(agg: AggregationBuffer): Unit = {
    agg.asInstanceOf[ProbSumAggBuffer].resultList = ProbSumUDAF.zero
  }

  override def iterate(
      agg: AggregationBuffer,
      parameters: Array[Object]
  ): Unit = {
    if (
      parameters == null || parameters.length != 2 || parameters(
        0
      ) == null || parameters(1) == null
    ) {
      return
    }

    val buffer = agg.asInstanceOf[ProbSumAggBuffer]
    val sumVal =
      PrimitiveObjectInspectorUtils.getDouble(parameters(0), inputSumOI);
    val bddVal = inputBddOI.getPrimitiveWritableObject(parameters(1))

    val inputBdd = new BDD(bddVal.getBytes())

    buffer.resultList =
      ProbSumUDAF.reduce(buffer.resultList, (sumVal, inputBdd))
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial == null) return

    val buffer = agg.asInstanceOf[ProbSumAggBuffer]
    val partialStructList =
      loi.getList(partial).asInstanceOf[java.util.List[ArrayList[AnyRef]]]

    val rightConvAgg = partialStructList.asScala
      .map(b =>
        (
          b.get(0).asInstanceOf[DoubleWritable].get(),
          new BDD(b.get(1).asInstanceOf[BytesWritable].getBytes())
        )
      )
      .toList

    buffer.resultList = ProbSumUDAF
      .merge(buffer.resultList, rightConvAgg)
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = terminate(agg)

  override def terminate(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[ProbSumAggBuffer]
    if (buffer.resultList == null || buffer.resultList.isEmpty) {
      return null
    }

    buffer.resultList
      .map({
        case (num, bdd) => {
          val res = new ArrayList[AnyRef]()
          res.add(new DoubleWritable(num))
          res.add(new BytesWritable(bdd.buffer))

          res
        }
      })
      .asJava
  }
}

class HiveProbSumGenericUDAF extends AbstractGenericUDAFResolver {
  @throws[HiveException]
  override def getEvaluator(
      parameters: Array[TypeInfo]
  ): GenericUDAFEvaluator = {
    if (parameters.length != 2) {
      throw new UDFArgumentException("prob_sum requires 2 arguments")
    }
    // Add more specific TypeInfo checks for DOUBLE and BINARY if desired
    new HiveProbSumGenericUDAFEvaluator()
  }

  @throws[HiveException]
  override def getEvaluator(
      info: GenericUDAFParameterInfo
  ): GenericUDAFEvaluator = {
    val parameters: Array[ObjectInspector] = info.getParameterObjectInspectors()
    if (parameters.length != 2) {
      throw new UDFArgumentException(
        "prob_sum requires 2 arguments (from GenericUDAFParameterInfo)"
      )
    }
    // Can add specific ObjectInspector checks here
    new HiveProbSumGenericUDAFEvaluator()
  }
}
