package uk.co.odinconsultants
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

class CrudSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

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
  }

}
