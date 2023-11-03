package uk.co.odinconsultants

import org.apache.spark.sql.SparkSession


object SmokeTestMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("phtest")
      .master("local[4]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val data = spark.range(0, 5)
    data.write.format("delta").save("/tmp/delta-table")
  }

}
