package com.yannmoisan.sparksql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class CombineAllTheThingsSpec extends FlatSpec with DataFrameSuiteBase with Matchers {
  "CombineAllTheThings" should "verify that helpers are composable" in {
    import sqlContext.implicits._

    val mapParent1 = Map(
      1 -> "elem1",
      2 -> "elem1",
      3 -> "elem1",
      4 -> "elem1",
      5 -> "elem2",
      6 -> "elem2",
      7 -> "parent",
      8 -> "parent",
      9 -> "old",
      10 -> "other1",
      11 -> "other2"
    )

    val mapChild1 = Map(
      7 -> Seq("child1"),
      8 -> Seq("child2", "child3")
    )

    val mapParent2 = Map(
      1 -> "elem1",
      2 -> "elem2",
      3 -> "old",
      4 -> "old"
    )
    val mapChild2 = Map.empty[Int, Seq[String]]

    val mapParent3 = Map.empty[Int, String]
    val mapChild3 = Map.empty[Int, Seq[String]]

    val df = sqlContext.sparkContext
      .parallelize(
        Seq(
          (1, mapParent1, mapChild1),
          (2, mapParent2, mapChild2),
          (3, mapParent3, mapChild3)
        )
      )
      .toDF("id", "parent", "child")

    val combine = new CombineMaps[String, Double](StringType, DoubleType, _ + _)
    val values = Maps.map { (t : (Int, String)) => t._2 }
    val values2 = Maps.map { (t : (String, Double)) => t._2 }
    val seqValues = Maps.map { (t : (Int, Seq[String])) => t._2 }

    val others = Maps.filter { (t : (String, Double)) => !Seq("elem1", "elem2", "child1", "child2", "empty").contains(t._1)}
    val merge = Arrays.merge[String]
    val head = Arrays.map((s: Seq[String]) => s.head)
    val filterNot = Arrays.filter[String]((s: String) => !Seq("parent", "old").contains(s))
    val frequencies = Arrays.frequencies[String]
    def withDefault(col: Column, default: String) = when(functions.size(col) === 0, typedLit(Seq(default))).otherwise(col)
    val arrSum = Arrays.sum[Double]

    val resultDF = df
      .withColumn("values", merge(values($"parent"), head(seqValues($"child"))))
      .withColumn("freq", frequencies(withDefault(filterNot($"values"), "empty")))
      .agg(combine($"freq").as("freq"))
      .withColumn("others", arrSum(values2(others.apply($"freq"))))

    val result = resultDF
      .collect()
      .map(r => (r.getAs[Map[String, Double]]("freq"), r.getAs[Double]("others")))

    result shouldEqual Array((Map(
      "elem1" -> 0.9d,
      "elem2" -> 0.7d,
      "child1" -> 0.1d,
      "child2" -> 0.1d,
      "other1" -> 0.1d,
      "other2" -> 0.1d,
      "empty" -> 1d
    ), 0.2d))
  }
}