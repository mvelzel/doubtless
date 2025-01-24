package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  GenericUDAFEvaluator,
  AbstractGenericUDAFResolver,
  GenericUDAFParameterInfo
}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.{
  AggregationBuffer,
  AbstractAggregationBuffer
}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  PrimitiveObjectInspectorUtils,
  PrimitiveObjectInspectorFactory
}

class HiveBDDAggAndUDAF extends AbstractGenericUDAFResolver {
  override def getEvaluator(x: GenericUDAFParameterInfo) = {
    new HiveBDDAggAndEvaluator()
  }
}

class HiveBDDAggAndEvaluator extends GenericUDAFEvaluator {

  class BDDBuffer(var bdd: BDD) extends AbstractAggregationBuffer {}

  override def getNewAggregationBuffer(): BDDBuffer = new BDDBuffer(BDD("0"))

  override def reset(buffer: AggregationBuffer) = buffer match {
    case bddBuffer: BDDBuffer => bddBuffer.bdd = BDD("0")
    case _                    => {}
  }

  override def iterate(buffer: AggregationBuffer, parameters: Array[Object]) =
    buffer match {
      case bddBuffer: BDDBuffer => {
        assert(parameters.length == 1)

        val bytes = PrimitiveObjectInspectorUtils
          .getBinary(
            parameters(0),
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
          )
          .getBytes()
        bddBuffer.bdd = bddBuffer.bdd & (new BDD(bytes))
      }
      case _ => {}
    }

  override def terminatePartial(buffer: AggregationBuffer) = buffer match {
    case bddBuffer: BDDBuffer => bddBuffer.bdd.buffer
  }

  override def merge(buffer: AggregationBuffer, partial: Object) =
    buffer match {
      case bddBuffer: BDDBuffer =>
        partial match {
          case bytes: Array[Byte] => {
            bddBuffer.bdd = bddBuffer.bdd & (new BDD(bytes))
          }
          case _ => {}
        }
      case _ => {}
    }

  override def terminate(buffer: AggregationBuffer) = buffer match {
    case bddBuffer: BDDBuffer => bddBuffer.bdd.buffer
  }
}
