package org.j4g.covid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}

object CovidData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Metadata")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("210606COVID19MEXICO.csv")

    df.printSchema()
    df.filter("FECHA_DEF!='9999-99-99'")
      .withColumn("OLD", when(col("EDAD") >= 60, 1).otherwise(0))
      .withColumn("DATE", to_date(col("FECHA_DEF"), "yyyy-MM-dd"))
      .select(
        col("OLD"),
        year(col("DATE")).as("YEAR"),
        weekofyear(col("DATE")).as("WEEK")
      )
      .groupBy("OLD", "YEAR", "WEEK").count().as("COUNT")
      .coalesce(1)
      .orderBy("YEAR", "WEEK")
      .withColumn("SEMANA", concat(col("YEAR"), lit("-"), col("WEEK")))
      .select(
        col("OLD"),
        col("SEMANA"),
        col("COUNT")
      )
      .write.format("csv").save("results.csv")
  }
}
