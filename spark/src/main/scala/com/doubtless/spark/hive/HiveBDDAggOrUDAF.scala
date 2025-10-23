package com.doubtless.spark.hive

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.ql.metadata.HiveException
import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  AbstractGenericUDAFResolver,
  GenericUDAFEvaluator,
  GenericUDAFParameterInfo
}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.{
  AbstractAggregationBuffer,
  AggregationBuffer,
  Mode
}
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.BytesWritable

class BDDAggOrBuffer extends AbstractAggregationBuffer {
  var result: BDD = _
}

class HiveBDDAggOrUDAFEvaluator extends GenericUDAFEvaluator {
  private var bddOI: BinaryObjectInspector = _
  private var interOI: BinaryObjectInspector = _

  override def init(
      mode: Mode,
      parameters: Array[ObjectInspector]
  ): ObjectInspector = {
    super.init(mode, parameters)

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length != 1) {
        throw new UDFArgumentException(
          "agg_or requires 1 arguments (binary)"
        )
      }

      parameters(0) match {
        case oi: BinaryObjectInspector => bddOI = oi
        case _ =>
          throw new UDFArgumentException("Argument 1 (bdd) must be binary")
      }
    } else {
      parameters(0) match {
        case oi: BinaryObjectInspector => interOI = oi
        case _ =>
          throw new UDFArgumentException("Intermediate result must be binary")
      }
    }

    return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
  }

  override def getNewAggregationBuffer(): AggregationBuffer = {
    val res = new BDDAggOrBuffer()
    reset(res)
    res
  }

  override def reset(agg: AggregationBuffer): Unit = {
    agg.asInstanceOf[BDDAggOrBuffer].result = BDD.True
  }

  override def iterate(
      agg: AggregationBuffer,
      parameters: Array[Object]
  ): Unit = {
    if (parameters == null || parameters.length != 1 || parameters(0) == null)
      return

    val buffer = agg.asInstanceOf[BDDAggOrBuffer]
    val bddVal = bddOI.getPrimitiveWritableObject(parameters(0))

    val inputBdd = new BDD(bddVal.getBytes())
    buffer.result = buffer.result | inputBdd
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial == null) return

    val buffer = agg.asInstanceOf[BDDAggOrBuffer]
    val bddVal = interOI.getPrimitiveWritableObject(partial);

    val rightBdd = new BDD(bddVal.getBytes())

    buffer.result = buffer.result | rightBdd;
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[BDDAggOrBuffer]

    new BytesWritable(buffer.result.buffer);
  }

  override def terminate(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[BDDAggOrBuffer]
    if (buffer.result == null) {
      return null
    }

    new BytesWritable(buffer.result.buffer);
  }
}

class HiveBDDAggOrUDAF extends AbstractGenericUDAFResolver {
  @throws[HiveException]
  override def getEvaluator(
      parameters: Array[TypeInfo]
  ): GenericUDAFEvaluator = {
    if (parameters.length != 1) {
      throw new UDFArgumentException("agg_or requires 1 argument")
    }

    new HiveBDDAggOrUDAFEvaluator()
  }

  @throws[HiveException]
  override def getEvaluator(
      info: GenericUDAFParameterInfo
  ): GenericUDAFEvaluator = {
    val parameters: Array[ObjectInspector] = info.getParameterObjectInspectors()
    if (parameters.length != 1) {
      throw new UDFArgumentException(
        "agg_or requires 1 argument (from GenericUDAFParameterInfo)"
      )
    }

    new HiveBDDAggOrUDAFEvaluator()
  }
}
