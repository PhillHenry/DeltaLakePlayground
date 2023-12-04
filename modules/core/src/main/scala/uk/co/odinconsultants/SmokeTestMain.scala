package uk.co.odinconsultants
import uk.co.odinconsultants.SparkUtils.{sparkSession, write}

object SmokeTestMain {

  def main(args: Array[String]): Unit = {
    val spark = sparkSession
    write(spark)
    ()
  }

}
