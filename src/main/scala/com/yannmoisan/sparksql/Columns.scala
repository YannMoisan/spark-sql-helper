package com.yannmoisan.sparksql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when

object Columns {
  def whenIsNull(col: Column, default: Any): Column = when(col.isNull, default).otherwise(col)

  def safeSum(cols: Seq[Column]): Column = cols.map(whenIsNull(_, 0)).reduce(_ + _)
}
