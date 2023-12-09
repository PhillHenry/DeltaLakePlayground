package uk.co.odinconsultants
import org.apache.spark.sql.SaveMode
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.SimpleFixture.now
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}
import org.scalatest.matchers.should.Matchers._

class MergingDataSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import SparkUtils.sparkSession.implicits._

  "Data" should {
    "be merged" in new SimpleSparkFixture {
      val sinkSQL                     = createTableSQLUsingDelta(tableName)
      spark.sqlContext.sql(sinkSQL)
      Given(s"a table with ${data.length} rows")
      appendData(tableName)
      val mergeOp                     = "replaceWhere"
      private val newData: Seq[Datum] = createData(1, now, dayDelta, tsDelta)
      val partitionId                 = 0
      assert(newData.map(_.partitionKey).toSet.headOption == Some(partitionId))
      private val mergeCondition      = s"partitionKey = $partitionId"
      When(s"we use '$mergeOp' to write those partitions where $mergeCondition")
      val newDF                       = spark.createDataFrame(newData)
      newDF.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option(mergeOp, mergeCondition)
        .saveAsTable(tableName)
      Then(s"the partition IDs that are not $partitionId will not change but partition $partitionId will have the new rows")
      val partitionToCountOriginal    = partitionKeyToCount(data)
      val partitionToCountAfter       = partitionKeyToCount(spark.read.table(tableName).as[Datum].collect().toSeq)
      partitionToCountOriginal.foreach { case (key: Long, count: Int) =>
        if (key != partitionId) {
          count shouldEqual partitionToCountAfter(key)
        } else {
          count should be < partitionToCountAfter(key)
        }
      }
    }
  }

  private def partitionKeyToCount(
      data: Seq[Datum]
  ): Map[Long, Int] =
    data.map(_.partitionKey).foldLeft(Map.empty[Long, Int].withDefault(_ => 0)) {
      case (acc: Map[Long, Int], key: Long) => acc.updated(key, acc(key) + 1)
    }
}
