package uk.co.odinconsultants
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, WAREHOUSE_PATH}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.lang
import java.nio.file.Files

object SparkUtils {
  val tmpDir   : String    = Files.createTempDirectory("SparkForTesting").toString

  def getSession(app: String = "bdd_tests"): SparkSession = {
    val master   : String    = "local[2]"
    val sparkConf: SparkConf = {
      println(s"Using temp directory $tmpDir")
      System.setProperty("derby.system.home", tmpDir)
      new SparkConf()
        .setMaster(master)
        .setAppName(app)
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set(CATALOG_IMPLEMENTATION.key, "hive")
        .set("spark.sql.catalog.local.type", "hadoop")
//        .set(DEFAULT_CATALOG.key, "local")
        .set(WAREHOUSE_PATH.key, tmpDir)
        .setSparkHome(tmpDir)
    }
    SparkContext.getOrCreate(sparkConf)
    SparkSession
      .builder()
      .appName(app)
      .master("local[2]")
      .getOrCreate()
  }

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
