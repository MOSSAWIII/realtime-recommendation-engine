package core

import config.{DatabaseConfig, RabbitMQConfig}
import models._
import repositories._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._
import akka.stream.Materializer
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Main extends App {
  println("Real-Time Recommendation System")
  println("===============================")
  
  implicit val system = ActorSystem("recommendation-system")
  implicit val materializer = Materializer(system)
  
  // Check database and RabbitMQ connections
  if (!checkConnections()) {
    println("Unable to start application due to connection issues")
    System.exit(1)
  }
  
  try {
    // Initialize repositories
    val userRepo = new UserRepository()
    val eventRepo = new EventRepository()
    val participationRepo = new ParticipationRepository()
    val friendsRepo = new FriendsRepository()
    val interactionRepo = new InteractionRepository()
    
    // Initialize services
    val cacheManager = new CacheManager()
    val dataLoader = new DataLoader(userRepo, eventRepo, participationRepo, friendsRepo, cacheManager)
    val interactionService = new UserInteractionService(interactionRepo, cacheManager)
    val recommendationEngine = new RecommendationEngine(
      userRepo, eventRepo, participationRepo, friendsRepo, interactionRepo, cacheManager
    )
    
    // Start message consumers
    dataLoader.startConsumers()
    interactionService.startConsumer()
    recommendationEngine.startRequestConsumer()
    
    // Define JSON format for API requests/responses
    implicit val localDateTimeFormat = new JsonFormat[LocalDateTime] {
      def write(dt: LocalDateTime) = JsString(dt.toString)
      def read(value: JsValue) = value match {
        case JsString(dt) => LocalDateTime.parse(dt)
        case _ => throw DeserializationException("Expected ISO date format")
      }
    }
    
    implicit val interactionFormat = jsonFormat3(UserInteraction)  // Changed from jsonFormat4

    implicit val mapIntIntFormat = new JsonFormat[Map[Int, Int]] {
      def write(map: Map[Int, Int]) = {
        JsObject(map.map { case (k, v) => k.toString -> JsNumber(v) })
      }
      def read(value: JsValue) = value match {
        case JsObject(fields) => 
          fields.map { case (k, v) => k.toInt -> v.convertTo[Int] }.toMap
        case _ => throw DeserializationException("Map expected")
      }
    }

    implicit val eventFormat = jsonFormat11(Event)
    
    // Define API routes
    val routes = {
      path("health") {
        get {
          complete(StatusCodes.OK -> "Service is healthy")
        }
      } ~
      path("recommendations" / LongNumber) { userId =>
        get {
          parameter("count".as[Int].?(5)) { count =>
            onSuccess(recommendationEngine.generateRecommendations(userId, count)) { recommendations =>
              complete(recommendations)
            }
          }
        }
      } ~
      path("interaction") {
        post {
          entity(as[UserInteraction]) { interaction =>
            onSuccess(interactionService.recordInteraction(
              interaction.userId, 
              interaction.eventId, 
              interaction.interactionType
            )) { success =>
              if (success) complete(StatusCodes.Created -> "Interaction recorded")
              else complete(StatusCodes.InternalServerError -> "Failed to record interaction")
            }
          }
        }
      }
    }
    
    // Start the HTTP server
    val bindingFuture = Http().bindAndHandle(routes, "0.0.0.0", 8080)
    
    println(s"Server online at http://0.0.0.0:8080/")
    println("Press RETURN to stop...")
    scala.io.StdIn.readLine()
    
    // Graceful shutdown
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => {
        cleanupResources(dataLoader, interactionService, recommendationEngine, cacheManager)
        system.terminate()
      })
    
  } catch {
    case e: Exception =>
      println(s"Critical error: ${e.getMessage}")
      e.printStackTrace()
  }
  
  private def checkConnections(): Boolean = {
    val dbConnected = Await.result(DatabaseConfig.healthCheck(), 10.seconds)
    val mqConnected = RabbitMQConfig.healthCheck()
    
    println(s"Database connection: ${if (dbConnected) "✅" else "❌"}")
    println(s"RabbitMQ connection: ${if (mqConnected) "✅" else "❌"}")
    
    dbConnected && mqConnected
  }
  
  private def cleanupResources(
    dataLoader: DataLoader,
    interactionService: UserInteractionService,
    recommendationEngine: RecommendationEngine,
    cacheManager: CacheManager
  ): Unit = {
    println("Shutting down resources...")
    dataLoader.close()
    interactionService.close()
    recommendationEngine.close()
    cacheManager.close()
    RabbitMQConfig.close()
    DatabaseConfig.close()
    println("Resources shut down successfully")
  }
}
