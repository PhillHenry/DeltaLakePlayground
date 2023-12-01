import sbt.{Def, _}
import org.portablescala.sbtplatformdeps.PlatformDepsPlugin.autoImport._

object Dependencies {

  object V {
    val fs2Core            = "3.2.7"
    val fs2Kafka           = "2.4.0"
    val circe              = "0.14.2"
    val scalacheck         = "1.16.0"
    val organizeImports    = "0.6.0"
    val spark              = "3.5.0"
    val hadoopAws          = "3.3.1"
    val minio              = "8.5.5"
    val deltaLake          = "3.0.0"
    val documentationUtils = "0.6"
    val scalaTest          = "3.2.9"
  }

  object Libraries {
    def circe(artifact: String): Def.Initialize[sbt.ModuleID] =
      Def.setting("io.circe" %%% ("circe-" + artifact) % V.circe)

    val fs2Core = "co.fs2"        %% "fs2-core"        % V.fs2Core
    val logBack = "ch.qos.logback" % "logback-classic" % "1.2.11"

    // test
    val scalacheck = "org.scalacheck"  %% "scalacheck" % V.scalacheck
    // only for demo
    val fs2Kafka   = "com.github.fd4s" %% "fs2-kafka"  % V.fs2Kafka

    // scalafix rules
    val organizeImports = "com.github.liancheng" %% "organize-imports" % V.organizeImports

    val spark      = ("org.apache.spark" %% "spark-sql"  % V.spark).cross(CrossVersion.for3Use2_13)
    val sparkHive  = ("org.apache.spark" %% "spark-hive" % V.spark).cross(CrossVersion.for3Use2_13)
    val sparkKafka =
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % V.spark).cross(CrossVersion.for3Use2_13)
    val hadoopAws = ("org.apache.hadoop" % "hadoop-aws" % V.hadoopAws)
      .exclude("com.fasterxml.jackson.core", "jackson-databind")
    val minio     =
      ("io.minio" % "minio" % V.minio).exclude("com.fasterxml.jackson.core", "jackson-databind")
    val deltaLake = "io.delta" %% "delta-spark" % V.deltaLake

    val documentationUtilsScalaTest =
      "uk.co.odinconsultants.documentation_utils" % "scalatest_utils" % V.documentationUtils
    val documentationUtilsSpark =
      "uk.co.odinconsultants.documentation_utils" % "spark" % V.documentationUtils
    val documentationUtilsCore =
      "uk.co.odinconsultants.documentation_utils" % "spark" % V.documentationUtils

    val scalaTest       = "org.scalatest"       %% "scalatest"                % V.scalaTest
    val flexMark        = "com.vladsch.flexmark" % "flexmark"                 % "0.36.8" % Test
    val flexMarkPegMark = "com.vladsch.flexmark" % "flexmark-profile-pegdown" % "0.36.8" % Test
  }

}
