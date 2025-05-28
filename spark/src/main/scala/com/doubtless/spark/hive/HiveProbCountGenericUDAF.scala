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
import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  AbstractGenericUDAFResolver,
  GenericUDAFEvaluator,
  GenericUDAFParameterInfo
}
import com.doubtless.spark.ProbCountUDAF

class ProbCountAggBuffer extends AbstractAggregationBuffer {
  var resultList: List[BDD] = _
}

class HiveProbCountGenericUDAFEvaluator extends GenericUDAFEvaluator {
  // Input ObjectInspectors
  private var inputBddOI: BinaryObjectInspector = _

  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
  private var loi: ListObjectInspector = _;

  override def init(
      mode: Mode,
      parameters: Array[ObjectInspector]
  ): ObjectInspector = {
    super.init(mode, parameters)

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length != 1) {
        throw new UDFArgumentException(
          "prob_count requires 1 argument (binary)"
        )
      }

      parameters(0) match {
        case oi: BinaryObjectInspector => inputBddOI = oi
        case _ =>
          throw new UDFArgumentException("Argument 1 (bdd) must be binary")
      }
    } else {
      parameters(0) match {
        case oi: ListObjectInspector => loi = oi
        case _ =>
          throw new UDFArgumentException("Intermediate result must be list")
      }
    }

    return ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
    )
  }

  override def getNewAggregationBuffer(): AggregationBuffer = {
    val res = new ProbCountAggBuffer()
    reset(res)
    res
  }

  override def reset(agg: AggregationBuffer): Unit = {
    agg.asInstanceOf[ProbCountAggBuffer].resultList = ProbCountUDAF.zero
  }

  override def iterate(
      agg: AggregationBuffer,
      parameters: Array[Object]
  ): Unit = {
    if (parameters == null || parameters.length != 1 || parameters(0) == null)
      return

    val buffer = agg.asInstanceOf[ProbCountAggBuffer]
    val bddVal = inputBddOI.getPrimitiveWritableObject(parameters(0))

    val inputBdd = new BDD(bddVal.getBytes())
    buffer.resultList = ProbCountUDAF.reduce(buffer.resultList, inputBdd)
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial == null) return

    val buffer = agg.asInstanceOf[ProbCountAggBuffer]
    val partialBddList =
      loi.getList(partial).asInstanceOf[java.util.List[BytesWritable]]

    val rightConvAgg =
      partialBddList.asScala.map(b => new BDD(b.getBytes())).toList

    buffer.resultList = ProbCountUDAF
      .merge(buffer.resultList, rightConvAgg)
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    terminate(agg)
  }

  override def terminate(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[ProbCountAggBuffer]
    if (buffer.resultList == null || buffer.resultList.isEmpty) {
      return null
    }

    buffer.resultList
      .map(bdd => new BytesWritable(bdd.buffer))
      .asJava
  }
}

class HiveProbCountGenericUDAF extends AbstractGenericUDAFResolver {
  @throws[HiveException]
  override def getEvaluator(
      parameters: Array[TypeInfo]
  ): GenericUDAFEvaluator = {
    if (parameters.length != 1) {
      throw new UDFArgumentException("prob_count requires 1 argument")
    }
    // Add more specific TypeInfo checks for BINARY if desired
    new HiveProbCountGenericUDAFEvaluator()
  }

  @throws[HiveException]
  override def getEvaluator(
      info: GenericUDAFParameterInfo
  ): GenericUDAFEvaluator = {
    val parameters: Array[ObjectInspector] = info.getParameterObjectInspectors()
    if (parameters.length != 1) {
      throw new UDFArgumentException(
        "prob_count requires 1 argument (from GenericUDAFParameterInfo)"
      )
    }
    // Can add specific ObjectInspector checks here
    new HiveProbCountGenericUDAFEvaluator()
  }
}
