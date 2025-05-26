package application

import application.core._
import domain.models._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.persistence.Repository

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory

// Add the remainder of your Main.scala content here
