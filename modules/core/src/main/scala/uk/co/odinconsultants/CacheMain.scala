package uk.co.odinconsultants
import org.apache.spark.sql.DataFrame
import uk.co.odinconsultants.SparkUtils.{getSession, read, write}

import java.io.File

object CacheMain {

  /**
   * This demonstrates that if you .cache() a DeltaLake DataFrame then you will never see
   * any updates, even if you create a <b>new reference</b> to the directory.
   */
  def main(args: Array[String]): Unit = {
    val spark = getSession("ph_test")
    val dir = createTempDir()
    val n: Long = 5
    write(spark, dir, n)
    assert(read(spark, dir).count() == n)
    write(spark, dir, n)
    val df: DataFrame = read(spark, dir)
    assert(df.count() == n * 2)
    df.cache()
    write(spark, dir, n)
    assert(read(spark, dir).count() == n * 3)
    ()
  }
  private def createTempDir(): String = {
    val dir = File.createTempFile(this.getClass.getSimpleName.replace("$", "_"), "")
    dir.delete()
    println(s"${dir.getAbsoluteFile} Directory created? ${dir.mkdir()}")
    dir.getAbsolutePath
  }
}
