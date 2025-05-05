// This file contains additional settings for testing

// Add testing dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.mockito" % "mockito-core" % "4.6.1" % Test,
  "org.powermock" % "powermock-module-junit4" % "2.0.9" % Test,
  "org.powermock" % "powermock-api-mockito2" % "2.0.9" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.41.0" % Test,
  "org.testcontainers" % "rabbitmq" % "1.19.1" % Test
)

// Enable experimental ByteBuddy support (fixed deprecated 'in')
Test / javaOptions += "-Dnet.bytebuddy.experimental=true"

// Ensure test classes have access to main resources
Test / resourceDirectory := (Compile / resourceDirectory).value

// Show full stack traces on test failures
Test / testOptions += Tests.Argument("-oF")

// Fork tests to avoid interference between them
Test / fork := true

// Increase memory for tests
Test / javaOptions ++= Seq("-Xms512M", "-Xmx1G")

// Parallel test execution
Test / parallelExecution := false  // Set to true when tests don't interfere with each other

// Ensure clean before tests
Test / test := (Test / test).dependsOn(Test / clean).value

// Custom test commands
addCommandAlias(
  "runStreamingTest",
  ";clean ;testOnly integration.streaming.StreamingTransformerIntegrationSpec"
)

addCommandAlias(
  "runAllIntegrationTests",
  ";clean ;testOnly integration.**"
)

// Task to run Docker-dependent tests
lazy val dockerTests = taskKey[Unit]("Run tests with Docker dependencies")

dockerTests := {
  import scala.sys.process._
  
  val s = streams.value
  val log = s.log
  
  log.info("Stopping any running test containers...")
  "docker-compose -f docker-compose-test.yml down --remove-orphans".!
  
  log.info("Starting test containers...")
  "docker-compose -f docker-compose-test.yml up -d test-db test-rabbitmq".!
  
  log.info("Waiting for containers to initialize...")
  Thread.sleep(30000) // 30 seconds
  
  log.info("Running transformer integration test...")
  (Test / testOnly).toTask(" integration.streaming.StreamingTransformerIntegrationSpec").value
  
  log.info("Test completed, stopping containers...")
  "docker-compose -f docker-compose-test.yml down".!
}

// Extra settings (fixed deprecated 'in')
Test / javaOptions += "-Djdk.attach.allowAttachSelf=true"