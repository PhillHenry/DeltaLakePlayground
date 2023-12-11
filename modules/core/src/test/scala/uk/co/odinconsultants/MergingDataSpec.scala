package uk.co.odinconsultants
import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.schema.DeltaInvariantViolationException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers._
import uk.co.odinconsultants.documentation_utils.SimpleFixture.now
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

class MergingDataSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import SparkUtils.sparkSession.implicits._

  "Data" should {
    val histoColumns  = Seq("Partition Key", "Count")
    val partitionId   = 0L
    val mergeCondtion = s"partitionKey = $partitionId"
    "be merged" in new SimpleSparkFixture {
      spark.sqlContext.sql(createTableSQLUsingDelta(tableName))
      Given(s"a table with ${data.length} rows")
      appendData(tableName)

      private val dataWith1Partition: Seq[Datum] = createData(1, now, dayDelta, tsDelta)
      assert(dataWith1Partition.map(_.partitionKey).toSet.headOption == Some(partitionId))
      val partitionToCountOriginal               = partitionKeyToCount(data)

      whenWeMerge(spark, "replaceWhere", dataWith1Partition, mergeCondtion)
      And(
        s"the distribution of partition keys to row counts looks like:\n${histogram(partitionToCountOriginal, histoColumns)}"
      )

      Then(
        s"the partition IDs that are not $partitionId will not change but partition $partitionId will have the new rows"
      )
      val partitionToCountAfter =
        partitionKeyToCount(spark.read.table(tableName).as[Datum].collect().toSeq)
      And(
        s"""the distribution of partition keys to row counts looks like:
           |${histogram(partitionToCountAfter, histoColumns)}
           |where the data with partition key '$partitionId' has been upserted""".stripMargin
      )
      partitionToCountAfter(partitionId) shouldBe dataWith1Partition.length
      partitionToCountOriginal.foreach { case (key: Long, count: Int) =>
        if (key != partitionId) {
          count shouldEqual partitionToCountAfter(key)
        } else {
          count should be < partitionToCountAfter(key)
        }
      }
    }
    "not be merged if the partition key is not defined" in new SimpleSparkFixture {
      Given(
        s"a table with partition keys ${data.map(_.partitionKey).toSet.toList.sorted.mkString(", ")}"
      )
      try {
        whenWeMerge(spark, "replaceWhere", data, mergeCondtion)
        fail(s"Was expecting a ${DeltaInvariantViolationException.getClass.getSimpleName}")
      } catch {
        case x: DeltaAnalysisException =>
          x.getCause match {
            case _: DeltaInvariantViolationException =>
              Then(s"a ${DeltaInvariantViolationException.getClass.getSimpleName} is thrown")
            case _                                   =>
              fail(s"Was expecting a ${DeltaInvariantViolationException.getClass.getSimpleName}")
          }
      }
    }
  }

  def whenWeMerge(
      spark:          SparkSession,
      mergeOp:        String,
      newData:        Seq[Datum],
      mergeCondition: String,
  ): Unit = {
    When(s"we use '$mergeOp' to write ${newData.length} new rows that have partition keys {${newData
        .map(_.partitionKey)
        .mkString(", ")}} where $mergeCondition")
    val newDF = spark.createDataFrame(newData)
    newDF.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option(mergeOp, mergeCondition)
      .saveAsTable(tableName)
  }

  private def partitionKeyToCount(
      data: Seq[Datum]
  ): Map[Long, Int] =
    data.map(_.partitionKey).foldLeft(Map.empty[Long, Int].withDefault(_ => 0)) {
      case (acc: Map[Long, Int], key: Long) => acc.updated(key, acc(key) + 1)
    }
}
