package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  GenericUDAFEvaluator,
  AbstractGenericUDAFResolver,
  GenericUDAFParameterInfo,
  GenericUDAFResolver2
}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.{
  AggregationBuffer,
  AbstractAggregationBuffer
}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  PrimitiveObjectInspectorUtils,
  PrimitiveObjectInspectorFactory
}

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.exec.Description;

import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo

class HiveBDDAggAndUDAF extends UDAF { }
object HiveBDDAggAndUDAF {
  class HiveBDDAggAndEvaluator extends UDAFEvaluator {
    var bdd: BDD = BDD.True

    override def init() = {
      this.bdd = BDD.True
    }

    def iterate(bytes: BytesWritable) = {
      this.bdd = this.bdd & (new BDD(bytes.getBytes()))
      true
    }

    def terminatePartial() = terminate()

    def merge(bytes: BytesWritable) = {
      this.bdd = this.bdd & (new BDD(bytes.getBytes()))
      true
    }

    def terminate() = new BytesWritable(this.bdd.buffer)
  }
}

//@Description(
//  name = "bdd_agg_and",
//  value = "_FUNC_(x) - Returns the and of a set of BDDs"
//)
//class HiveBDDAggAndUDAF extends AbstractGenericUDAFResolver {
//  override def getEvaluator(x: GenericUDAFParameterInfo) = {
//    new HiveBDDAggAndUDAF.HiveBDDAggAndEvaluator()
//  }
//}
//object HiveBDDAggAndUDAF {
//  class HiveBDDAggAndEvaluator extends GenericUDAFEvaluator {
//    class BDDBuffer(var bdd: BytesWritable) extends AbstractAggregationBuffer {}
//
//    override def getNewAggregationBuffer(): BDDBuffer = new BDDBuffer(
//      new BytesWritable(BDD("0").buffer)
//    )
//
//    override def reset(buffer: AggregationBuffer) = {
//      val bddBuffer = buffer.asInstanceOf[BDDBuffer]
//      bddBuffer.bdd = new BytesWritable(BDD("0").buffer)
//    }
//
//    override def iterate(
//        buffer: AggregationBuffer,
//        parameters: Array[Object]
//    ) = {
//      assert(parameters.length == 1)
//      val bddBuffer = buffer.asInstanceOf[BDDBuffer]
//
//      val bdd = new BDD(bddBuffer.bdd.getBytes())
//      val bytes = PrimitiveObjectInspectorUtils
//        .getBinary(
//          parameters(0),
//          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
//        )
//        .getBytes()
//      bddBuffer.bdd = new BytesWritable((bdd & (new BDD(bytes))).buffer)
//    }
//
//    override def terminatePartial(buffer: AggregationBuffer) = terminate(buffer)
//
//    override def merge(buffer: AggregationBuffer, partial: Object) = {
//      val bddBuffer = buffer.asInstanceOf[BDDBuffer]
//      val bytes = partial.asInstanceOf[BytesWritable]
//      val bdd = new BDD(bddBuffer.bdd.getBytes())
//      bddBuffer.bdd = new BytesWritable(
//        (bdd & (new BDD(bytes.getBytes()))).buffer
//      )
//    }
//
//    override def terminate(buffer: AggregationBuffer) = {
//      val bddBuffer = buffer.asInstanceOf[BDDBuffer]
//      bddBuffer.bdd
//    }
//  }
//}
