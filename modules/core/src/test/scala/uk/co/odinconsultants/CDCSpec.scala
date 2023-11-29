package uk.co.odinconsultants
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

class CDCSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  "A dataset that is CDC enabled" should {
    val sinkTable: String = "myDeltaTable"
    val tableName: String = Datum.getClass.getSimpleName.replace("$", "")
    "TODO do something" in new SimpleSparkFixture {
//      import spark.implicits._
      createTable(tableName, spark).show()
      createTable(sinkTable, spark)
      val deltaDF = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)
        .option("mergeSchema", "true")
//        .option("endingVersion", 10)
        .table(tableName)
      spark.createDataFrame(data).writeTo(tableName).append()
      spark.createDataFrame(data).write.format("delta").mode(SaveMode.Append).saveAsTable(tableName)
      deltaDF.show()
//      deltaDF.drop('_commit_version, '_change_type, '_commit_timestamp).writeTo(sinkTable).append()
      val targetDF = DeltaTable.forName(sinkTable)
      targetDF
        .merge(deltaDF, s"$tableName.id = $sinkTable.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .whenNotMatchedBySource()
        .delete()
        .execute()
      targetDF.toDF.show()
//       TODO
    }
  }

  private def createTable(tableName: String, spark: SparkSession): DataFrame = {
    val createSql: String = s"""CREATE TABLE $tableName (id INT, label STRING, partitionKey LONG)
                                      |USING DELTA
                                      |""".stripMargin
    val alterTableSql: String =
      s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    spark.sqlContext.sql(createSql)
    spark.sqlContext.sql(alterTableSql)
    spark.sqlContext.sql(s"DESCRIBE HISTORY $tableName")
  }
}
