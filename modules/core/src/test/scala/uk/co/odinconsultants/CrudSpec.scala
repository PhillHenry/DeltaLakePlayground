package uk.co.odinconsultants
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

class CrudSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  "A Delta table" should {
    "be created and populated" in new SimpleSparkFixture {
      val sinkSQL = createTableSQLUsingDelta(tableName)
      Given(s"a table created with SQL${formatSQL(sinkSQL)}")
      spark.sqlContext.sql(sinkSQL)
    }
  }

}
