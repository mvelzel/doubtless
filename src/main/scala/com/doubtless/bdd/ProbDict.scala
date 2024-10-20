package com.doubtless.bdd

import scala.collection.immutable.{Map, MapOps}

case class RandVar(name: String, value: Int) {
  require(!name.matches("^.*[:=;].*$"), "variable name cannot contain ':', '=', or ';'.")
  require(name.length <= 11, "variable name cannot be longer than 11 characters.")

  override def toString(): String = s"$name=$value"
}

object RandVar {
  def apply(name: String, value: Int) = new RandVar(name, value)

  def apply(varDef: String) = {
    val Pattern = "([^=]*)=(.*)".r
    varDef match {
      case Pattern(name, value) =>
        value.toIntOption match {
          case Some(intValue) => new RandVar(name, intValue)
          case None =>
            throw new IllegalArgumentException(
              "the variable values must be an integer."
            )
        }
      case _ =>
        throw new IllegalArgumentException(
          "the variable definition must contain '='."
        )
    }
  }
}

class ProbDict private (
    private[bdd] val buffer: Array[Byte],
    private val varKeys: Set[RandVar]
) extends Map[RandVar, Double] {
  private def this(buffer: Array[Byte]) =
    this(buffer, Native.getKeys(buffer).map(RandVar(_)).toSet)
  def this(varDefs: String) = this(Native.createDict(varDefs))

  override def keys: Iterable[RandVar] = varKeys

  override def toString(): String =
    Native.printDict(buffer).filter(_ >= ' ').trim

  override def iterator: Iterator[(RandVar, Double)] = ???

  override def removed(key: RandVar): ProbDict = {
    if (varKeys contains key) {
      new ProbDict(
        Native.modifyDict(buffer, 2, key.toString())
      )
    } else {
      this
    }
  }

  override def updated[V1 >: Double](key: RandVar, value: V1): ProbDict = ???

  override def get(key: RandVar): Option[Double] = {
    if (varKeys contains key) {
      Some(Native.lookupProb(buffer, key.name, key.value))
    } else {
      None
    }
  }
}
