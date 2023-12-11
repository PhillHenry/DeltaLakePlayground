package uk.co.odinconsultants
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers._
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

class ChangeDataFlowSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info(
    "See https://www.databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html"
  )

  "A dataset that is CDC enabled" should {
    val sinkTable: String = "myDeltaTable"
    val pkCol: String     = "id"
    val condition: String = s"$tableName.$pkCol = $sinkTable.$pkCol"
    "be created and populated" in new SimpleSparkFixture {
      Given(aCDFTable(tableName, spark))

      When(s"we write ${data.length} rows to $tableName")
      appendData(tableName)
      And(s"again write another ${data.length} rows to $tableName")
      appendData(tableName)

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

    "write its deltas to another table in a batch" in new SimpleSparkFixture {
      val sinkSQL = createTableSQLUsingDelta(sinkTable)
      Given(s"a sink table created with SQL: ${formatSQL(sinkSQL)}")
      spark.sqlContext.sql(sinkSQL)

      val sourceDF = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .option("mergeSchema", "true")
        .table(tableName)

      val sinkDF              = DeltaTable.forName(spark, sinkTable)
      When(s"we merge on the condition ${Console.CYAN}$condition${Console.RESET}")
      ChangeDataFlowSpec.doMerge(sourceDF, sinkDF, condition)
      val numTargetRows: Long = sinkDF.toDF.count()
      Then(s"the rows in the sink file are not unique, in fact there are $numTargetRows rows")
      assert(sourceDF.count() == data.size * 2)
      assert(numTargetRows == data.size * 2)
      And(s"the sink table looks like this:\n${captureOutputOf {
          sinkDF.toDF.orderBy(pkCol).show(truncate = false)
        }}")
      info(
        "See https://stackoverflow.com/questions/69562007/databricks-delta-table-merge-is-inserting-records-despite-keys-are-matching-with"
      )
    }
  }
}

object ChangeDataFlowSpec {
  def upsertToDelta(sinkDF:    DeltaTable,
                    condition: String)(df: DataFrame, batchId: Long): Unit = {
    println(s"batchId = $batchId")
    doMerge(df, sinkDF, condition)
  }

  private def doMerge(
      sourceDF:  DataFrame,
      sinkDF:    DeltaTable,
      condition: String,
  ): Unit =
    sinkDF
      .merge(sourceDF, condition)
      .whenMatched(condition)
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .whenNotMatchedBySource()
      .delete()
      .execute()
}