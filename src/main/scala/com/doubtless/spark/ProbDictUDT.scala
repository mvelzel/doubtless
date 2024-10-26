package com.doubtless.spark

import com.doubtless.bdd._
import org.apache.spark.sql.types.{UserDefinedType, DataType, BinaryType}

class ProbDictUDT extends UserDefinedType[ProbDict] {
  override def userClass: Class[ProbDict] = classOf[ProbDict]

  override def deserialize(datum: Any): ProbDict = datum match {
    case buffer: Array[Byte] => new ProbDict(buffer)
    case _ =>
      throw new IllegalArgumentException(
        "the ProbDict to deserialize is not of type Array[Byte]."
      )
  }

  override def serialize(obj: ProbDict): Any = obj.buffer

  override def sqlType: DataType = BinaryType

}
