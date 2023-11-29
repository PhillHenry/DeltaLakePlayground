package uk.co.odinconsultants
import org.apache.spark.sql.SaveMode
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.documentation_utils.{Datum, SpecPretifier, TableNameFixture}

class CDCSpec extends SpecPretifier with GivenWhenThen with TableNameFixture {

  "A dataset that is CDC enabled" should {
    val tableName: String = Datum.getClass.getSimpleName.replace("$", "")
    val createSql: String = s"""CREATE TABLE $tableName (id INT, label STRING, partitionKey LONG)
                                      |USING DELTA
                                      |""".stripMargin
    val alterTableSql: String = s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    "TODO do something" in new SimpleSparkFixture {
      spark.sqlContext.sql(createSql)
      spark.sqlContext.sql(alterTableSql)
//      spark.createDataFrame(data).writeTo(tableName).append()
      spark.createDataFrame(data).write.format("delta").mode(SaveMode.Append).saveAsTable(tableName)
      // TODO
    }
  }

}
