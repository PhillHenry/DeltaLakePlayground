package uk.co.odinconsultants

import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{DoNotDiscover, GivenWhenThen}
import org.scalatest.matchers.should.Matchers._
import uk.co.odinconsultants.documentation_utils.SQLUtils.createTableSQL
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

@DoNotDiscover
class ChangeDataFlowStreamingSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  info(
    "https://docs.databricks.com/en/structured-streaming/delta-lake.html"
  )

  "A dataset that is updated" should {
    "write its deltas to another table as a stream" in new SimpleSparkFixture {
      givenCDFTable(tableName, spark)
      override def num_rows: Int         = 100
      val streamSinkTable                = "streamsink"
      val sinkSQL                        = createTableSQLUsingDelta(streamSinkTable)
      And(s"a sink table created with SQL: ${formatSQL(sinkSQL)}")
      spark.sqlContext.sql(sinkSQL)
      val pauseMs                        = 4000L
      val future: Future[StreamingQuery] = whenStreaming(spark, tableName, streamSinkTable, pauseMs * 3)
      val sinkDF                         = DeltaTable.forName(spark, streamSinkTable).toDF
      private val initialCount: Long     = sinkDF.count()
      And(s"the initial count in $streamSinkTable is 0")
      initialCount shouldEqual 0
      override val data                  =
        createData(num_partitions, new java.sql.Date(new java.util.Date().getTime), dayDelta, 1000)
      And(
        s"we append $num_rows rows with a timestamp ranging from ${data.map(_.timestamp).min} to ${data.map(_.timestamp).max}"
      )
      appendData(tableName)
      And(s"we wait $pauseMs ms")
      Thread.sleep(pauseMs)

      val query                          = Await.result(future, FiniteDuration.apply(pauseMs * 4, TimeUnit.MILLISECONDS))
      Then(s"the final row count at ${new java.util.Date()} in $streamSinkTable is ${sinkDF.count()} rows")
      sinkDF.count() shouldEqual data.length
      query.stop()
    }
  }

  def whenStreaming(
      spark:         SparkSession,
      sourceTable:   String,
      sinkTable:     String,
      terminationMs: Long,
  ): Future[StreamingQuery] = {
    val watermarkSeconds              = 4
    val waterMark                     = s"$watermarkSeconds seconds"
    val triggerProcessTimeMS          = 4000L
    When(s"we start streaming from $sourceTable to $sinkTable with a watermark of $waterMark and a trigger processing time of $triggerProcessTimeMS ms")
    implicit val ec: ExecutionContext = ExecutionContext.Implicits.global
    Future {
      val query = spark.readStream
        .format("delta")
        .option("withEventTimeOrder", "true")
        .table(sourceTable)
        .withWatermark("timestamp", waterMark)
        .writeStream
        .option("checkpointLocation", s"${SparkUtils.tmpDir}/${sinkTable}checkpoint")
        .format("delta")
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime(triggerProcessTimeMS))
        .toTable(sinkTable)

      query.awaitTermination(terminationMs)
      query
    }
  }

  def givenCDFTable(tableName: String, spark: SparkSession): DataFrame = {
    val createCDF: String =
      s"${createTableSQLUsingDelta(tableName)} TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    Given(s"a table created with the SQL: ${formatSQL(createCDF)}")
    spark.sqlContext.sql(createCDF)
  }

  def createTableSQLUsingDelta(tableName: String): String =
    s"""${createTableSQL(tableName, classOf[Datum])}
                                                     |USING DELTA""".stripMargin
}

