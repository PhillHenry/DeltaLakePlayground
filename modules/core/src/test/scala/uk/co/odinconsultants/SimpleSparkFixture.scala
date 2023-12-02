package uk.co.odinconsultants
import org.apache.spark.sql.SparkSession
import uk.co.odinconsultants.SparkUtils.tmpDir
import uk.co.odinconsultants.documentation_utils.SimpleFixture

trait SimpleSparkFixture extends SimpleFixture {

  val spark: SparkSession = SparkUtils.getSession()

  def dataDir(tableName: String): String = s"$tmpDir/$tableName/data"

  def appendData(tableName: String): Unit = spark.createDataFrame(data).writeTo(tableName).append()

}
