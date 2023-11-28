package uk.co.odinconsultants
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.lang

object SparkUtils {
  def getSession(app: String = "bdd_tests"): SparkSession =
    SparkSession
      .builder()
      .appName(app)
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

  def write(spark: SparkSession, dir: String ="/tmp/delta-table", n: Long = 5): Dataset[lang.Long] = {
    val data = spark.range(0, n)
    data.write.mode("overwrite").format("delta").save(dir)
    data
  }

  def read(spark: SparkSession, dir: String ="/tmp/delta-table"): DataFrame = {
    val data = spark.read.parquet(dir)
    data
  }
}
