package uk.co.odinconsultants
import org.apache.spark.sql.SparkSession
import uk.co.odinconsultants.documentation_utils.SimpleFixture

import java.nio.file.Files

trait SimpleSparkFixture extends SimpleFixture {

  val tmpDir   : String    = Files.createTempDirectory("SparkForTesting").toString

  val spark: SparkSession = SparkUtils.getSession()

  def dataDir(tableName: String): String = s"$tmpDir/$tableName/data"

}
