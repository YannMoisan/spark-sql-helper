package com.yannmoisan.sparksql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

class MapsSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  "map" should "manage missing keys" in {
    import sqlContext.implicits._

    val map1 = Map("k1" -> "A", "k2" -> "A", "k3" -> "B")
    val map2 = Map("k1" -> "A")

    val df = sqlContext.sparkContext
      .parallelize(
        Seq(
          (1, map1),
          (2, map2)
        )
      )
      .toDF("id", "map")

    val valuesUdf = Maps.map( (t: (String, String)) => t._2)

    val values = df
      .withColumn("values", valuesUdf($"map"))
      .collect()
      .map(r => r.getAs[Seq[String]]("values"))

    values shouldEqual Array(Seq("A", "A", "B"), Seq("A"))
  }

  "map" should "manage null" in {
    import sqlContext.implicits._

    val df = sqlContext.sparkContext
      .parallelize(
        Seq(
          (1, Map.empty[String, String]),
          (2, null)
        )
      )
      .toDF("id", "map")

    val valuesUdf = Maps.map( (t: (String, String)) => t._2)

    val values = df
      .withColumn("values", valuesUdf($"map"))
      .collect()
      .map(r => r.getAs[Seq[String]]("values"))

    values shouldEqual Array(Seq(), null)
  }

}
