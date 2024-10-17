package com.doubtless.bdd

class ProbDict(val buffer: Array[Byte]) {
  def this(varDefs: String) = this(Native.createDict(varDefs))

  override def toString(): String = Native.dict2string(buffer)
}
