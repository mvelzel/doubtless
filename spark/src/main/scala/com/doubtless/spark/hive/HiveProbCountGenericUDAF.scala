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
import org.apache.hadoop.hive.serde2.objectinspector._
import scala.jdk.CollectionConverters._
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.udf.generic.{
  AbstractGenericUDAFResolver,
  GenericUDAFEvaluator,
  GenericUDAFParameterInfo
}
import com.doubtless.spark.ProbCountUDAF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector

class ProbCountAggBuffer extends AbstractAggregationBuffer {
  var resultList: List[(BDD, String)] = _
}

class HiveProbCountGenericUDAFEvaluator extends GenericUDAFEvaluator {
  // Input ObjectInspectors
  private var inputBddOI: BinaryObjectInspector = _
  private var inputPruneMethodOI: StringObjectInspector = _

  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
  private var loi: ListObjectInspector = _

  private val interStructFieldNames: java.util.List[String] =
    java.util.List.of("bdd", "pruneMethod")

  override def init(
      mode: Mode,
      parameters: Array[ObjectInspector]
  ): ObjectInspector = {
    super.init(mode, parameters)

    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length != 2) {
        throw new UDFArgumentException(
          "prob_count requires 2 arguments (binary, string)"
        )
      }

      parameters(0) match {
        case oi: BinaryObjectInspector => inputBddOI = oi
        case _ =>
          throw new UDFArgumentException("Argument 1 (bdd) must be binary")
      }

      parameters(1) match {
        case oi: StringObjectInspector => inputPruneMethodOI = oi
        case _ =>
          throw new UDFArgumentException(
            "Argument 2 (pruneMethod) must be a string"
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
      return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
      )
    }
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
    if (
      parameters == null || parameters.length != 2 || parameters(
        0
      ) == null || parameters(1) == null
    )
      return

    val buffer = agg.asInstanceOf[ProbCountAggBuffer]
    val bddVal = inputBddOI.getPrimitiveWritableObject(parameters(0))

    val pruneMethodVal =
      PrimitiveObjectInspectorUtils.getString(parameters(1), inputPruneMethodOI)

    val inputBdd = new BDD(bddVal.getBytes())
    buffer.resultList =
      ProbCountUDAF.reduce(buffer.resultList, (inputBdd, pruneMethodVal))
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial == null) return

    val buffer = agg.asInstanceOf[ProbCountAggBuffer]
    val partialStructList =
      loi.getList(partial).asInstanceOf[java.util.List[ArrayList[AnyRef]]]

    val rightConvAgg =
      partialStructList.asScala
        .map(b => {
          val pruneMethod = b.get(1)
          val bdd =
            if (b.get(0) == null)
              null
            else
              new BDD(b.get(0).asInstanceOf[BytesWritable].getBytes())

            (
              bdd,
              if (pruneMethod == null) null
              else pruneMethod.asInstanceOf[Text].toString()
            )
        })
        .toList

    buffer.resultList = ProbCountUDAF
      .merge(buffer.resultList, rightConvAgg)
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[ProbCountAggBuffer]

    buffer.resultList
      .map(tup => {
        val res = new ArrayList[AnyRef]()
        res.add(
          if (tup._1 == null)
            null
          else
            new BytesWritable(tup._1.buffer)
        )
        res.add(if (tup._2 == null) null else new Text(tup._2))

        res
      })
      .asJava
  }

  override def terminate(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[ProbCountAggBuffer]
    if (buffer.resultList == null || buffer.resultList.isEmpty) {
      return null
    }

    ProbCountUDAF
      .finish(buffer.resultList)
      .map(bdd => {
        if (bdd == null)
          null
        else
          new BytesWritable(bdd.buffer)
      })
      .asJava
  }
}

class HiveProbCountGenericUDAF extends AbstractGenericUDAFResolver {
  @throws[HiveException]
  override def getEvaluator(
      parameters: Array[TypeInfo]
  ): GenericUDAFEvaluator = {
    if (parameters.length != 2) {
      throw new UDFArgumentException("prob_count requires 2 arguments")
    }

    new HiveProbCountGenericUDAFEvaluator()
  }

  @throws[HiveException]
  override def getEvaluator(
      info: GenericUDAFParameterInfo
  ): GenericUDAFEvaluator = {
    val parameters: Array[ObjectInspector] = info.getParameterObjectInspectors()
    if (parameters.length != 2) {
      throw new UDFArgumentException(
        "prob_count requires 2 arguments (from GenericUDAFParameterInfo)"
      )
    }

    new HiveProbCountGenericUDAFEvaluator()
  }
}
