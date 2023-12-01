package uk.co.odinconsultants
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers._
import uk.co.odinconsultants.documentation_utils.SQLUtils.createTableSQL
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

import java.io.ByteArrayOutputStream

class ChangeDataFlowSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info("See https://www.databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html")

  "A dataset that is CDC enabled" should {
    val sinkTable: String = "myDeltaTable"
    val tableName: String = Datum.getClass.getSimpleName.replace("$", "")
    "is created and populated" in new SimpleSparkFixture {
      givenCDFTable(tableName, spark)

      When(s"we write ${data.length} rows to $tableName")
      spark.createDataFrame(data).writeTo(tableName).append()
      And(s"again write another ${data.length} rows to $tableName")
      spark.createDataFrame(data).writeTo(tableName).append()

      val history                  = describeHistory(tableName, spark)
      val expectedHistoryRows: Int = 3
      Then(
        s"the history table has $expectedHistoryRows rows, 1 for creation and ${expectedHistoryRows - 1} for insertion"
      )
      history.count() shouldEqual expectedHistoryRows
      And(s"the history of the source table looks like:\n${captureOutputOf {
          history.show(truncate = false)
        }}")
    }

    "write its deltas to another table" in new SimpleSparkFixture {
      val sinkSQL = createTableSQLUsingDelta(sinkTable)
      Given(s"a sink table created with SQL: ${formatSQL(sinkSQL)}")
      spark.sqlContext.sql(sinkSQL)

      val deltaDF = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .option("mergeSchema", "true")
        .table(tableName)

      val targetDF            = DeltaTable.forName(sinkTable)
      val pkCol               = "id"
      val condition           = s"$tableName.$pkCol = $sinkTable.$pkCol"
      When(s"we merge on the condition ${Console.CYAN}$condition${Console.RESET}")
      targetDF
        .merge(deltaDF, condition)
        .whenMatched(condition)
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .whenNotMatchedBySource()
        .delete()
        .execute()
      val numTargetRows: Long = targetDF.toDF.count()
      Then(s"the rows in the sink file are not unique, in fact there are $numTargetRows rows")
      assert(deltaDF.count() == data.size * 2)
      assert(numTargetRows == data.size * 2)
      And(s"the sink table looks like this:\n${captureOutputOf {
          targetDF.toDF.orderBy(pkCol).show(truncate = false)
        }}")
      info("See https://stackoverflow.com/questions/69562007/databricks-delta-table-merge-is-inserting-records-despite-keys-are-matching-with")
    }
  }

  def captureOutputOf[T](thunk: => T): String = {
    val out = new ByteArrayOutputStream()
    Console.withOut(out) {
      thunk
    }
    new String(out.toByteArray)
  }

  def givenCDFTable(tableName: String, spark: SparkSession): DataFrame = {
    val createCDF: String =
      s"${createTableSQLUsingDelta(tableName)} TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    Given(s"a table created with the SQL: ${formatSQL(createCDF)}")
    spark.sqlContext.sql(createCDF)
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