package com.yannmoisan.sparksql

import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe.TypeTag

object Maps {
  def map[K: TypeTag, V: TypeTag, B: TypeTag](f: ((K, V)) => B) =
    udf { (m: Map[K, V]) => Option(m).map(_.map(f).toSeq) }

  def filter[K: TypeTag, V: TypeTag](f: ((K, V)) => Boolean) =
    udf { (m: Map[K, V]) => Option(m).map(_.filter(f)) }

  def divide[K: TypeTag, V: TypeTag : Numeric] = udf { (m: Map[K, V], n: Int) =>
    val num = implicitly[Numeric[V]]
    Option(m).map(_.mapValues(v => num.toDouble(v) / n))
  }

}
