package com.yannmoisan.sparksql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class ArraysSpec extends FlatSpec with DataFrameSuiteBase with Matchers {
  "frequencies" should "compute the frequence of each element in the array" in {
    import sqlContext.implicits._

    val df = sqlContext.sparkContext
      .parallelize(
        Seq(
          (1, Seq("A", "A", "A", "B", "C")),
          (2, Seq("A", "B"))
        )
      )
      .toDF("id", "array")

    val result = df
      .withColumn("frequencies", Arrays.frequencies[String].apply($"array"))
      .collect()
      .map(r => (r.getAs[Map[String, Double]]("frequencies")))

    result shouldEqual Array(
      Map("A" -> 0.6d, "B" -> 0.2d, "C" -> 0.2d),
      Map("A" -> 0.5d, "B" -> 0.5d)
    )
  }

}
