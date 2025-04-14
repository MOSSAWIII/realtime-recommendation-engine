name := "realtime-recommendation-engine"
version := "0.1"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-async" % "1.0.1",
  "com.typesafe" % "config" % "1.4.2",
  "org.postgresql" % "postgresql" % "42.2.20",
  "com.rabbitmq" % "amqp-client" % "5.15.0",
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

fork in run := true

mainClass in Compile := Some("core.Main")