package uk.co.odinconsultants
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.co.odinconsultants.SparkUtils.tmpDir
import uk.co.odinconsultants.documentation_utils.SQLUtils.createTableSQL
import uk.co.odinconsultants.documentation_utils.{Datum, SimpleFixture, SpecFormats}

trait SimpleSparkFixture extends SimpleFixture with SpecFormats {

  val spark: SparkSession = SparkUtils.sparkSession

  def dataDir(tableName: String): String = s"$tmpDir/$tableName/data"

  def appendData(tableName: String): Unit = spark.createDataFrame(data).writeTo(tableName).append()

  def aCDFTable(tableName: String, spark: SparkSession): String = {
    val createCDF: String =
      s"${createTableSQLUsingDelta(tableName)} TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    spark.sqlContext.sql(createCDF)
    s"a table created with the SQL: ${formatSQL(createCDF)}"
  }

  def describeHistory(
      tableName: String,
      spark:     SparkSession,
  ): DataFrame =
    spark.sqlContext.sql(s"DESCRIBE HISTORY $tableName")

  def createTableSQLUsingDelta(tableName: String): String =
    s"""${createTableSQL(tableName, classOf[Datum])}
       |USING DELTA""".stripMargin

}
