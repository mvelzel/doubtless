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
import com.doubtless.spark.ProbMaxUDAF
import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  AbstractGenericUDAFResolver,
  GenericUDAFEvaluator,
  GenericUDAFParameterInfo
}

class ProbMaxAggBuffer extends AbstractAggregationBuffer {
  var resultList: List[(Option[Double], BDD)] = _
}

class HiveProbMaxGenericUDAFEvaluator extends GenericUDAFEvaluator {
  // Input ObjectInspectors
  private var inputMaxOI: DoubleObjectInspector = _
  private var inputBddOI: BinaryObjectInspector = _

  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
  private var loi: ListObjectInspector = _;

  // Output ObjectInspectors
  private val structFieldNames: java.util.List[String] =
    java.util.List.of("max", "bdd")

  override def init(
      mode: Mode,
      parameters: Array[ObjectInspector]
  ): ObjectInspector = {
    super.init(mode, parameters)

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length != 2) {
        throw new UDFArgumentException(
          "prob_max requires 2 arguments (double, binary)"
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
      parameters == null || parameters.length != 2 || parameters(
        0
      ) == null || parameters(1) == null
    ) {
      return
    }

    val buffer = agg.asInstanceOf[ProbMaxAggBuffer]
    val maxVal =
      PrimitiveObjectInspectorUtils.getDouble(parameters(0), inputMaxOI);
    val bddVal = inputBddOI.getPrimitiveWritableObject(parameters(1))

    val inputBdd = new BDD(bddVal.getBytes())

    buffer.resultList =
      ProbMaxUDAF.reduce(buffer.resultList, (Option(maxVal), inputBdd))
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial == null) return

    val buffer = agg.asInstanceOf[ProbMaxAggBuffer]
    val partialStructList =
      loi.getList(partial).asInstanceOf[java.util.List[ArrayList[AnyRef]]]

    val rightConvAgg = partialStructList.asScala
      .map(b =>
        (
          Option(b.get(0).asInstanceOf[DoubleWritable]).map(d => d.get()),
          new BDD(b.get(1).asInstanceOf[BytesWritable].getBytes())
        )
      )
      .toList

    buffer.resultList = ProbMaxUDAF
      .merge(buffer.resultList, rightConvAgg)
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[ProbMaxAggBuffer]

    buffer.resultList
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
    if (parameters.length != 2) {
      throw new UDFArgumentException("prob_max requires 2 arguments")
    }
    // Add more specific TypeInfo checks for DOUBLE and BINARY if desired
    new HiveProbMaxGenericUDAFEvaluator()
  }

  @throws[HiveException]
  override def getEvaluator(
      info: GenericUDAFParameterInfo
  ): GenericUDAFEvaluator = {
    val parameters: Array[ObjectInspector] = info.getParameterObjectInspectors()
    if (parameters.length != 2) {
      throw new UDFArgumentException(
        "prob_max requires 2 arguments (from GenericUDAFParameterInfo)"
      )
    }
    // Can add specific ObjectInspector checks here
    new HiveProbMaxGenericUDAFEvaluator()
  }
}
