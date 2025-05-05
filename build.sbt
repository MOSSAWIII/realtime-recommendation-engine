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
  "org.powermock" % "powermock-module-junit4" % "2.0.9" % Test,
  "org.powermock" % "powermock-api-mockito2" % "2.0.9" % Test,
  "org.slf4j" % "slf4j-api" % "2.0.7",
  "org.junit.vintage" % "junit-vintage-engine" % "5.9.2" % Test,
  "org.scalatestplus" %% "junit-4-13" % "3.2.15.0" % Test
)

enablePlugins(JavaAppPackaging)

Test / fork := true
javaOptions in Test ++= Seq(
  "-Dnet.bytebuddy.experimental=true",
  "-Dorg.powermock.bytebuddy.mode=true",
  "-Djdk.attach.allowAttachSelf=true"
)

// Additional configuration for PowerMock with ScalaTest
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")