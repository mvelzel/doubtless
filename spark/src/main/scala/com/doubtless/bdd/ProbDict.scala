package com.doubtless.bdd

import com.doubtless.spark.ProbDictUDT
import org.apache.spark.sql.types.SQLUserDefinedType

case class RandVar(name: String, value: Int) {
  require(
    !name.matches("^.*[:=;].*$"),
    "variable name cannot contain ':', '=', or ';'."
  )
  require(
    name.length <= 11,
    "variable name cannot be longer than 11 characters."
  )

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

@SQLUserDefinedType(udt = classOf[ProbDictUDT])
class ProbDict private (
    val buffer: Array[Byte],
    private val varKeys: Set[RandVar]
) extends Map[RandVar, Double] {
  def this(buffer: Array[Byte]) =
    // Disable this for experiment efficientcy
    // this(buffer, Native.getKeys(buffer).map(RandVar(_)).toSet)
    this(buffer, Set())

  override def keys: Iterable[RandVar] = varKeys

  override def toString(): String =
    Native.printDict(buffer).filter(_ >= ' ').trim

  override def iterator: Iterator[(RandVar, Double)] =
    varKeys.iterator.map(randVar =>
      (randVar, Native.lookupProb(buffer, randVar.name, randVar.value))
    )

  override def removed(key: RandVar): ProbDict = {
    if (varKeys contains key) {
      new ProbDict(
        Native.modifyDict(buffer, 2, key.toString())
      )
    } else {
      this
    }
  }

  override def updated[V1 >: Double](key: RandVar, value: V1): ProbDict = {
    if (varKeys contains key) {
      new ProbDict(Native.modifyDict(buffer, 3, s"${key.toString}:$value"))
    } else {
      new ProbDict(Native.modifyDict(buffer, 1, s"${key.toString}:$value"))
    }
  }

  override def get(key: RandVar): Option[Double] = {
    if (varKeys contains key) {
      Some(Native.lookupProb(buffer, key.name, key.value))
    } else {
      None
    }
  }

  // We would like to use Native.mergeDicts, but this method does not allow overwriting random variables.
  //
  // override def concat[V2 >: Double](
  //     suffix: IterableOnce[(RandVar, V2)]
  // ): ProbDict = suffix match {
  //   case dict: ProbDict => new ProbDict(Native.modifyDict(buffer, 3, dict.toString()))
  //   case _ =>
  //     new ProbDict(
  //       Native.mergeDicts(buffer, ProbDict(suffix.iterator.toSeq*).buffer)
  //     )
  // }
  override def concat[V2 >: Double](
      suffix: IterableOnce[(RandVar, V2)]
  ): ProbDict = ProbDict(super.concat(suffix).toSeq: _*)
}

object ProbDict {
  // We would like not to have to sort here, but there is a bug in DuBio.
  // A string like "x=1:0.9;y=1:0.9;y=2:0.1;x=2:0.1;x=3:0.4"
  // gives different results from "x=1:0.9;x=2:0.1;x=3:0.4;y=1:0.9;y=2:0.1".
  // TODO Make a GitHub issue for this
  def apply[V2 >: Double](elems: (RandVar, V2)*) = new ProbDict(
    Native.createDict(
      elems
        .sortBy({ case (v, p) => (v.name, v.value) })
        .map({ case (v, p) => s"$v:$p" })
        .mkString(";")
    )
  )

  def apply(varDef: String) = new ProbDict(Native.createDict(varDef))
}
