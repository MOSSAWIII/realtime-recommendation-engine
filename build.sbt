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
  "com.zaxxer" % "HikariCP" % "5.0.1", 
  "ch.qos.logback" % "logback-classic" % "1.4.11", 
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.mockito" % "mockito-core" % "4.6.1" % Test,
  "org.slf4j" % "slf4j-api" % "2.0.7",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.41.0" % Test,
  "org.testcontainers" % "rabbitmq" % "1.19.1" % Test,
  "com.typesafe.akka" %% "akka-actor" % "2.6.20",
  "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test
)

enablePlugins(JavaAppPackaging)

Test / fork := true

// Additional test configuration
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")