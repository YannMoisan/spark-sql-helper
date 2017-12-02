package com.yannmoisan.sparksql

import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

object Arrays {
  def map[A: TypeTag, B: TypeTag](f: A => B) =
    udf { (a: Seq[A]) => Option(a).map(_.map(f)) }

  def filter[A: TypeTag](f: A => Boolean) =
    udf { (a: Seq[A]) => Option(a).map(_.filter(f)) }

  def sum[A: TypeTag: Numeric] =
    udf { (a: Seq[A]) => a.sum }

  def frequencies[A: TypeTag] =
    udf { (m: Seq[A]) =>
      m.groupBy(identity).mapValues(_.size.toDouble / m.size)
    }

  def merge[A: TypeTag] =
    udf { (a: Seq[A], b: Seq[A]) =>
      if (b == null) a else a ++ b
    }
}
