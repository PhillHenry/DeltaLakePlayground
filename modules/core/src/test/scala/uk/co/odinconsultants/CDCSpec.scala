package uk.co.odinconsultants
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

class CDCSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  "A dataset that is CDC enabled" should {
    val createSql: String = """CREATE TABLE student (id INT, name STRING, age INT)
                                      |TBLPROPERTIES (delta.enableChangeDataFeed = true)
                                      |""".stripMargin
//    val alterTableSql: String = "ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
//    val enableSql: String = "set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;"
    "TODO do something" in new SimpleSparkFixture {
      spark.sqlContext.sql(createSql)
//      spark.sqlContext.sql(alterTableSql)
//      spark.sqlContext.sql(enableSql)
      // TODO
    }
  }

}
