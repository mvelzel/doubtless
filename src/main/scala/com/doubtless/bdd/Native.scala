package com.doubtless.bdd

import com.github.sbt.jni.syntax.NativeLoader

private[bdd] object Native extends NativeLoader("bdd") {
  @native def createBdd(expr: String): Array[Byte]
  @native def bdd2string(bdd: Array[Byte]): String
  @native def bddOperator(
      operator: String,
      leftBdd: Array[Byte],
      rightBdd: Array[Byte]
  ): Array[Byte]
  @native def bddProb(dict: Array[Byte], bdd: Array[Byte]): Double
  @native def bddEqual(leftBdd: Array[Byte], rightBdd: Array[Byte]): Boolean
  @native def bddEquiv(leftBdd: Array[Byte], rightBdd: Array[Byte]): Boolean

  @native def createDict(varDefs: String): Array[Byte]
  @native def dict2string(dict: Array[Byte]): String
}
