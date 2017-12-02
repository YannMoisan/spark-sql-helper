package com.yannmoisan.sparksql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class CombinedMapsSpec extends FlatSpec with DataFrameSuiteBase with Matchers {
  "CombinedMaps" should "combine all kind of maps" in {
    import sqlContext.implicits._

    def udaf = new CombineMaps[String, Int](StringType, IntegerType, _ + _)

    val df = sqlContext.sparkContext
      .parallelize(
        Seq(
          (1, Map("A" -> 1, "B" -> 2)),
          (2, Map("A" -> 2, "C" -> 4)),
          (3, Map.empty[String, Int])
        )
      )
      .toDF("id", "map")

    val result = df
      .agg(udaf($"map") as "map")
      .collect()
      .map(r => (r.getAs[Map[String, Double]]("map")))

    result shouldEqual Array(
      Map("A" -> 3, "B" -> 2, "C" -> 4)
    )
  }

}
