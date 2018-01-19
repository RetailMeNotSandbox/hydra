enablePlugins(JavaAppPackaging, UniversalPlugin, PlayScala, SbtWeb, BuildInfoPlugin)

name := """Hydra"""

organization := "com.rmn.api"

scalaVersion := "2.11.6"

resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  filters,
  "com.typesafe.play" %% "play-slick" % "2.0.2",
  "com.typesafe.play" %% "play-slick-evolutions" % "2.0.2",
  "com.github.tminglei" %% "slick-pg" % "0.14.4",
  "com.github.tminglei" %% "slick-pg_play-json" % "0.14.4",
  "com.github.tminglei" %% "slick-pg_joda-time" % "0.14.4",
  "com.typesafe.slick" %% "slick-codegen" % "3.1.0",
  "com.github.mauricio" %% "postgresql-async" % "0.2.21",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "nl.grons" %% "metrics-scala" % "3.5.4_a2.3",
  "io.dropwizard.metrics" % "metrics-json" % "3.1.2",
  "io.dropwizard.metrics" % "metrics-jvm" % "3.1.2",
  "org.postgresql" % "postgresql" % "9.4.1209",
    /* We already had a dependency on org.postgresql.postgresql, via slick-pg.
       But slick-pg 0.14.4 pulls postgresql 9.4-1201-jdbc41, which causes the following issue:
       https://github.com/tminglei/slick-pg/issues/220#issuecomment-162125808 */
  "com.miguno.akka" % "akka-mock-scheduler_2.11" % "0.4.0" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.14" % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.14" % "test",
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.0" % "test",
  "com.rmn" %% "play-jsonapi" % "0.1",
   specs2 % Test
)

// Play provides two styles of routers, one expects its actions to be injected, the
// other, legacy style, accesses its actions statically.
routesGenerator := InjectedRoutesGenerator

fork in run := true

buildInfoPackage := "hydra.build"

buildInfoUsePackageAsPath := true

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoOptions ++= Seq(BuildInfoOption.ToMap, BuildInfoOption.ToJson, BuildInfoOption.BuildTime)

// Release
import ReleaseTransformations._
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
  runClean,                               // : ReleaseStep
  runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  setNextVersion,                         // : ReleaseStep
  commitNextVersion                       // : ReleaseStep
)
