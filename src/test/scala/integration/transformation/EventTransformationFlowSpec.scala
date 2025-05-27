// filepath: src/test/scala/integration/transformation/EventTransformationFlowSpec.scala
// Créer ce nouveau fichier de test d'intégration

package integration.transformation

import common.base.IntegrationSpec
import domain.models.{Event, ProcessedEvent}
import infrastructure.messaging.rabbitmq.RabbitMQClient
import application.core.Transformer
import scala.concurrent.duration._
import scala.concurrent.Await

class EventTransformationFlowSpec extends IntegrationSpec {
  "Event transformation flow" should {
    "process and publish events correctly" in {
      // Setup test data and expected transformations
      // Verify complete flow from raw event to published recommendation
    }
  }
}

