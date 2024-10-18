package com.doubtless.bdd

import scala.collection.immutable.{Map, MapOps}

class ProbDict private (private[bdd] val buffer: Array[Byte])
    extends Map[String, Double] {
  def this(varDefs: String) = this(Native.createDict(varDefs))

  override def toString(): String = {
    println(Native.getKeys(buffer).toList)
    Native.printDict(buffer).filter(_ >= ' ').trim
  }

  override def iterator: Iterator[(String, Double)] = ???

  override def removed(key: String): ProbDict = new ProbDict(
    Native.modifyDict(buffer.clone(), 2, key)
  )

  override def updated[V1 >: Double](key: String, value: V1): ProbDict = ???

  override def get(key: String): Option[Double] = ???
}
