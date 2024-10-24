package com.doubtless.spark

import com.doubtless.bdd._
import org.apache.spark.sql.types.{UserDefinedType, DataType, BinaryType}

class BDDUserDefinedType extends UserDefinedType[BDD] {
  override def userClass: Class[BDD] = classOf[BDD]

  override def deserialize(datum: Any): BDD = datum match {
    case buffer: Array[Byte] => new BDD(buffer)
    case _ =>
      throw new IllegalArgumentException(
        "the BDD to deserialize is not of type Array[Byte]."
      )
  }

  override def serialize(obj: BDD): Any = obj.buffer

  override def sqlType: DataType = BinaryType

}
