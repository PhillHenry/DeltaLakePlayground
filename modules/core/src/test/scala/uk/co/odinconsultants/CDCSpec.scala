package uk.co.odinconsultants
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

class CDCSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  "A dataset that is CDC enabled" should {
    "TODO do something" in new SimpleSparkFixture {
      spark.sqlContext.sql("""CREATE TABLE student (id INT, name STRING, age INT)
                             |TBLPROPERTIES (delta.enableChangeDataFeed = true)
                             |""".stripMargin)
      // TODO
    }
  }

}
