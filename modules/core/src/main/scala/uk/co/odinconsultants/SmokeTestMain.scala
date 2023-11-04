package uk.co.odinconsultants

import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit.MINUTES
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object SmokeTestMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSession("ph_test")
    val data  = spark.range(0, 5)
    data.write.mode("overwrite").format("delta").save("/tmp/delta-table")
  }

}
