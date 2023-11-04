package uk.co.odinconsultants
import uk.co.odinconsultants.SparkUtils.{getSession, write}

object SmokeTestMain {

  def main(args: Array[String]): Unit = {
    val spark = getSession("ph_test")
    write(spark)
    ()
  }

}
