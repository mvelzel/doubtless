package com.doubtless.bdd

class ProbDict private (private[bdd] val buffer: Array[Byte]) {
  def this(varDefs: String) = this(Native.createDict(varDefs))

  override def toString(): String = Native.dict2string(buffer)
}
