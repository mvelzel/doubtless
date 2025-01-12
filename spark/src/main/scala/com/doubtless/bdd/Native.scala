package com.doubtless.bdd

import com.github.sbt.jni.nativeLoader

@nativeLoader("bdd")
private[bdd] object Native {
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
  @native def bddGenerateDot(bdd: Array[Byte]): String

  @native def createDict(varDefs: String): Array[Byte]
  @native def dict2string(dict: Array[Byte]): String
  @native def printDict(dict: Array[Byte]): String
  @native def modifyDict(
      dict: Array[Byte],
      mode: Int,
      dictDef: String
  ): Array[Byte]
  @native def getKeys(dict: Array[Byte]): Array[String]
  @native def lookupProb(
      dict: Array[Byte],
      varName: String,
      varVal: Int
  ): Double
  @native def mergeDicts(
      leftDict: Array[Byte],
      rightDict: Array[Byte]
  ): Array[Byte]
}
