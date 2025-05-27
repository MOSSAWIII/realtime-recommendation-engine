// This file contains additional settings for testing

// Add testing dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.mockito" % "mockito-core" % "4.6.1" % Test,
  // Suppression des dépendances PowerMock qui ne sont plus nécessaires
  // "org.powermock" % "powermock-module-junit4" % "2.0.9" % Test,
  // "org.powermock" % "powermock-api-mockito2" % "2.0.9" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.41.0" % Test,
  "org.testcontainers" % "rabbitmq" % "1.19.1" % Test
)

// Ensure test classes have access to main resources
Test / resourceDirectory := (Compile / resourceDirectory).value

// Show full stack traces on test failures
Test / testOptions += Tests.Argument("-oF")

// Fork tests to avoid interference between them
Test / fork := true

// Increase memory for tests
Test / javaOptions ++= Seq("-Xms512M", "-Xmx1G")

// Parallel test execution
Test / parallelExecution := true

// But limit the number of tests in parallel to avoid exhausting resources
Test / concurrentRestrictions := Seq(
  Tags.limit(Tags.CPU, 4),
  Tags.limit(Tags.Network, 2),
  Tags.limit(Tags.Test, 8)
)

// Enable test grouping by package for better isolation
Test / testGrouping := {
  import Tests._
  
  val tests = (Test / definedTests).value
  
  val packageGroups = tests.groupBy { test =>
    val parts = test.name.split('.')
    if (parts.length > 2) parts.take(2).mkString(".")
    else if (parts.length > 1) parts(0)
    else "default"
  }.toSeq
  
  packageGroups.map { case (pkg, tests) =>
    Group(pkg, tests, SubProcess(ForkOptions()))
  }
}

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

// Nouveaux alias de commande adaptés à la nouvelle structure
addCommandAlias(
  "testUnit",
  ";clean ;testOnly unit.**"
)

addCommandAlias(
  "testIsolated",
  ";clean ;testOnly **/isolated/**"
)

addCommandAlias(
  "testFast",
  ";testOnly **/fast/**"
)

// Task to run Docker-dependent tests
lazy val dockerTests = taskKey[Unit]("Run tests with Docker dependencies")

dockerTests := {
  import scala.sys.process._
  import java.util.UUID  // Ajout de l'import manquant
  
  val s = streams.value
  val log = s.log
  
  // Générer un ID de test unique
  val testRunId = s"${System.currentTimeMillis()}_${UUID.randomUUID().toString.take(8)}"
  System.setProperty("test.runId", testRunId)
  
  log.info(s"Running tests with unique ID: $testRunId")
  log.info("Stopping any running test containers...")
  "docker-compose -f docker-compose-test.yml down --remove-orphans".!
  
  log.info("Starting test containers...")
  s"""docker-compose -f docker-compose-test.yml up -d 
      --scale test-db=1 --scale test-rabbitmq=1 
      --name test-db-${testRunId} 
      --name test-rabbitmq-${testRunId}""".!
  
  log.info("Waiting for containers to initialize...")
  Thread.sleep(20000) // 20 seconds
  
  // Export test ID for container references
  s"export TEST_RUN_ID=${testRunId}".!
  
  log.info("Running integration tests...")
  (Test / testOnly).toTask(" integration.**").value
  
  log.info("Test completed, stopping containers...")
  "docker-compose -f docker-compose-test.yml down".!
}

// Extra settings (fixed deprecated 'in')
Test / javaOptions += "-Djdk.attach.allowAttachSelf=true"