package uk.co.odinconsultants
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

import scala.collection.mutable.{Set => MSet}

class DeltaLakeCRUDSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  import SparkUtils.sparkSession.implicits._

  val files: MSet[String] = MSet.empty[String]

  "A Delta table" should {
    "be created and populated" in new SimpleSparkFixture {
      val sinkSQL = createTableSQLUsingDelta(tableName)
      Given(s"a table created with SQL${formatSQL(sinkSQL)}")
      spark.sqlContext.sql(sinkSQL)
      When(s"we write ${data.length} rows to $tableName")
      appendData(tableName)
      Then("the table indeed contains all the data")
      assertDataIn(tableName)
      And(s"the metastore contains a reference to the table $tableName")
      assert(spark.sessionState.catalog.tableExists(TableIdentifier(tableName)))
      assert(spark.sessionState.catalog.externalCatalog.getTable("default", tableName) != null)
    }

    val newVal = "ipse locum"
    val updateSql = s"update $tableName set label='$newVal'"
    s"support updates with '$updateSql'" in new SimpleSparkFixture {
      Given(s"SQL ${formatSQL(updateSql)}")
      When("we execute it")
      spark.sqlContext.sql(updateSql)
      Then("all rows are updated")
      val output: Dataset[Datum]  = spark.read.table(tableName).as[Datum]
      val rows: Array[Datum]      = output.collect()
      And(s"look like:\n${prettyPrintSampleOf(rows)}")
      assert(rows.length == data.length)
      for {
        row <- rows
      } yield assert(row.label == newVal)
      val dataFiles: List[String] = dataFilesIn(tableName)
      assert(dataFiles.length > files.size)
      files.addAll(dataFiles)
    }

    val newColumn = "new_string"
    val alterTable =
    s"ALTER TABLE $tableName ADD COLUMNS ($newColumn string comment '$newColumn docs')"
      s"be able to have its schema updated" in new SimpleSparkFixture {
      Given(s"SQL ${formatSQL(alterTable)}")
      When("we execute it")
      spark.sqlContext.sql(alterTable)
      Then("all rows are updated")
      val output: DataFrame = spark.read.table(tableName)
      val rows: Array[Row]  = output.collect()
      And(s"look like:\n${prettyPrintSampleOf(rows)}")
      for {
        row <- rows
      } yield assert(row.getAs[String](newColumn) == null)
    }
  }

}
