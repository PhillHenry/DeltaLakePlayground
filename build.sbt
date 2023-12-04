import Dependencies._

ThisBuild / scalaVersion     := "2.13.12"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "uk.co.odinconsultants"
ThisBuild / organizationName := "OdinConsultants"

ThisBuild / evictionErrorLevel := Level.Warn
ThisBuild / scalafixDependencies += Libraries.organizeImports

ThisBuild / resolvers += Resolver.sonatypeRepo("snapshots")

Compile / run / fork           := true

Global / onChangedBuildSource := ReloadOnSourceChanges

val sparkAndKafka = Seq(
  Libraries.spark,
  Libraries.sparkHive,
  Libraries.sparkKafka,
  Libraries.hadoopAws,
  Libraries.deltaLake,
)

val commonDependencies = Seq(
  Libraries.logBack,
  Libraries.minio,
  Libraries.documentationUtilsScalaTest,
  Libraries.documentationUtilsSpark,
  Libraries.documentationUtilsCore,
  Libraries.scalaTest,
) ++ sparkAndKafka

val commonSettings = List(
  scalacOptions ++= List(),
  scalafmtOnCompile := false, // recommended in Scala 3
  testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
)

lazy val root = (project in file("."))
  .settings(
    name := "DeltaLakePlayground"
  )
  .aggregate(lib, core, it)

lazy val lib = (project in file("modules/lib"))
  .settings((commonSettings ++ List(libraryDependencies := commonDependencies)): _*)

lazy val core = (project in file("modules/core"))
  .settings(
    commonSettings ++ List(
      libraryDependencies := commonDependencies,
      testOptions         := Seq(
        Tests.Argument(TestFrameworks.ScalaTest, "-fW", "mdocs/scenarios.txt")
      ),
    ): _*
  )
  .dependsOn(lib)

// integration tests
lazy val it = (project in file("modules/it"))
  .settings(commonSettings: _*)
  .dependsOn(core)
  .settings(
    libraryDependencies ++= List(
      "ch.qos.logback" % "logback-classic" % "1.2.11" % Test
    ) ++ commonDependencies
  )

val bddDocs = taskKey[Unit]("Turn the BDD output into HTML")

val header = """## DeltaLake Playground
               |
               |These are BDD (Behaviour Driven Design) tests that both test
               |the code and generate human readable documentation.
               |
               |""".stripMargin
val args = " uk.co.odinconsultants.documentation_utils.SplitScenariosMain \"" + header + "\" mdocs/scenarios.txt"

bddDocs := Def.taskDyn {
  val appName = name.value
  Def.task {
    (runMain in core in Compile)
      .toTask(args)
      .value
  }
}.value

lazy val docs = project
  .in(file("docs"))
  .settings(
    mdocIn        := file("modules/docs"),
    mdocOut       := file("target/docs"),
    mdocVariables := Map("VERSION" -> version.value),
  )
  .dependsOn(core)
  .enablePlugins(MdocPlugin)

addCommandAlias("runLinter", ";scalafixAll --rules OrganizeImports")
