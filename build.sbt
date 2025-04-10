name := "realtime-recommendation-engine"
version := "1.0"
scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  // Slick for database access
  "com.typesafe.slick" %% "slick" % "3.4.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
  
  // PostgreSQL driver
  "org.postgresql" % "postgresql" % "42.6.0",
  
  // Configuration
  "com.typesafe" % "config" % "1.4.2",
  
  // Redis client
  "redis.clients" % "jedis" % "4.3.1",
  
  // RabbitMQ client
  "com.rabbitmq" % "amqp-client" % "5.16.0",
  
  // Akka HTTP for API
  "com.typesafe.akka" %% "akka-http" % "10.5.0",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.5.0",
  "com.typesafe.akka" %% "akka-stream" % "2.7.0",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.7.0",
  
  // JSON handling
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5",
  
  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Assembly settings for creating a fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}