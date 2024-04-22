package uk.co.odinconsultants
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import uk.co.odinconsultants.SparkUtils.sparkSession

import java.io.File

object CacheMain {

  /** This demonstrates that if you .cache() a DeltaLake DataFrame then you will never see
    * any updates, even if you create a <b>new reference</b> to the directory.
    */
  def main(args: Array[String]): Unit                                            = {
    val spark         = sparkSession
    val dir           = createTempDir()
    val n: Long       = 5
    write(spark, dir, n, 0)
    assert(reaDelta(spark, dir).count() == n)
    write(spark, dir, n, 1)
    val df: DataFrame = reaDelta(spark, dir)
    assert(df.count() == n * 2)
    df.cache()
    write(spark, dir, n, 1)
    assert(reaDelta(spark, dir).count() == n * 3)
    ()
  }
  def reaDelta(spark: SparkSession, dir: String = "/tmp/delta-table"): DataFrame = {
    val data = spark.read.format("delta").load(dir)
    data
  }
  private def createTempDir(): String                                            = {
    val dir = File.createTempFile(this.getClass.getSimpleName.replace("$", "_"), "")
    dir.delete()
    println(s"${dir.getAbsoluteFile} Directory created? ${dir.mkdir()}")
    dir.getAbsolutePath
  }

  case class MyRow(myPartition: Int, x: Long)

  def write(
      spark:     SparkSession,
      dir:       String = "/tmp/delta-table",
      n:         Long = 5,
      partition: Int,
  ): Dataset[MyRow] = {
    import SparkUtils.sparkSession.implicits._
    val data = spark.range(0, n).map(x => MyRow(partition, x.toLong))
    data.write.partitionBy("myPartition").mode("append").format("delta").save(dir)
    data
  }
}
