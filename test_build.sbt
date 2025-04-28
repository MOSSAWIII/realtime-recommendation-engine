// This file contains additional settings for testing

// Add testing dependencies
libraryDependencies ++= Seq(
  // --> This line brings in mockito-core 4.6.1 <--
  "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.41.0" % Test,
  "org.testcontainers" % "rabbitmq" % "1.19.1" % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,

  // Add PowerMock dependencies
  "org.powermock" % "powermock-module-junit4" % "2.0.9" % Test,
  "org.powermock" % "powermock-api-mockito2" % "2.0.9" % Test
)

// Enable experimental ByteBuddy support
Test / javaOptions += "-Dnet.bytebuddy.experimental=true"
javaOptions in Test += "-Dnet.bytebuddy.experimental=true"

// Ensure test classes have access to main resources
Test / resourceDirectory := (Compile / resourceDirectory).value

// Fix import issues by making the following changes to your test files:
// 1. Add to StreamingTransformerIntegrationSpec.scala:
//    import scala.concurrent.ExecutionContext

// 2. Add to StreamingConsumerSpec.scala:
//    import models.ProcessedEvent
//    And fix the MockTransformer class to not use private method transformEvent

// 3. Fix Recommendation constructor in RepositorySpec.scala:
//    val recommendation = Recommendation(
//      id = Some(1),
//      userId = 42,
//      recommendedItems = List(RecommendedItem(101, 0.9, "test")),
//      score = 0.95, // Add this missing parameter
//      timestamp = System.currentTimeMillis()
//    )

// 4. Fix ambiguous implicit in RepositorySpec.scala:
//    class TestableRepository(mockConnection: Connection)(override implicit val ec: ExecutionContext) extends Repository
//    OR remove the global import

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

// Import section fixes:
// 1. Add to your test files that use ExecutionContext:
// import scala.concurrent.ExecutionContext
// import scala.concurrent.ExecutionContext.Implicits.global

// 2. Add to files that use ProcessedEvent:
// import models.ProcessedEvent

// 3. For files that have model construction issues:
// Check parameter lists and ensure all required fields are provided

// Remove or comment out this line if it exists
// "org.scalatest" %% "scalatest-concurrent" % "3.2.15" % Test