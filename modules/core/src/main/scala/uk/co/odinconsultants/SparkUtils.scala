package uk.co.odinconsultants
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSession(app: String): SparkSession =
    SparkSession
      .builder()
      .appName(app)
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
}
