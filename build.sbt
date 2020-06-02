ThisBuild / crossScalaVersions := Seq("2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-plugin-jdbc"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-plugin-jdbc"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-plugin-jdbc"),
  "scm:git@github.com:precog/quasar-plugin-jdbc.git"))

ThisBuild / publishAsOSSProject := true

val DoobieVersion = "0.9.0"
lazy val quasarVersion =
  Def.setting[String](managedVersions.value("precog-quasar"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-plugin-jdbc",
    libraryDependencies ++= Seq(
      "com.precog" %% "quasar-connector" % quasarVersion.value,

      "org.slf4s" %% "slf4s-api" % "1.7.25",

      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion
    ))


lazy val avalancheDatasource = project
  .in(file("avalanche/datasource"))
  .dependsOn(core)
  .settings(
    name := "quasar-datasource-avalanche",

    initialCommands in console := """
      import slamdata.Predef._

      import doobie._
      import doobie.implicits._
      import doobie.util.ExecutionContexts

      import cats._
      import cats.data._
      import cats.effect._
      import cats.implicits._

      implicit val contextShiftIO = IO.contextShift(ExecutionContexts.synchronous)

      val syncBlocker = Blocker.liftExecutionContext(ExecutionContexts.synchronous)
    """,

    quasarPluginName := "avalanche",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDatasourceFqcn := Some("quasar.plugin.avalanche.datasource.AvalancheDatasourceModule$"),
    quasarPluginDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.25",

      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion
    ),

    // FIXME: can't be using this in prod
    // Assemble a "fat" jar consisting of the plugin and the unmanaged iijdbc.jar
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp.filter(_.data.getName != "iijdbc.jar")
    },
    packageBin in Compile := (assembly in Compile).value
  )
  .enablePlugins(QuasarPlugin)
