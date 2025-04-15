name := "realtime-recommendation-engine"
version := "0.1.0"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client" % "5.16.0",
  "org.postgresql" % "postgresql" % "42.5.4",
  "com.typesafe" % "config" % "1.4.2",
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

enablePlugins(JavaAppPackaging)