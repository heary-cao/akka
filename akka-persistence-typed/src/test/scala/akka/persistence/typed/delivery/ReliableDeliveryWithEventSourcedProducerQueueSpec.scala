/*
 * Copyright (C) 2017-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.delivery

import java.util.UUID

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.ProducerController
import akka.persistence.typed.PersistenceId
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object ReliableDeliveryWithEventSourcedProducerQueueSpec {
  def conf: Config =
    ConfigFactory.parseString(s"""
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.journal.inmem.test-serialization = off # FIXME on
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/ProducerControllerWithEventSourcedProducerQueueSpec-${UUID
      .randomUUID()
      .toString}"
    """)
}

class ReliableDeliveryWithEventSourcedProducerQueueSpec
    extends ScalaTestWithActorTestKit(ReliableDeliveryWithEventSourcedProducerQueueSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  "ReliableDelivery with EventSourcedProducerQueue" must {

    "deliver messages after full producer and consumer restart" in {
      val producerId = "p1"
      val producerProbe = createTestProbe[ProducerController.RequestNext[String]]()

      val producerController = spawn(
        ProducerController[String](
          producerId,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController ! ProducerController.Start(producerProbe.ref)

      val consumerController = spawn(ConsumerController[String](resendLost = true))
      val consumerProbe = createTestProbe[ConsumerController.Delivery[String]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)
      consumerController ! ConsumerController.RegisterToProducerController(producerController)

      producerProbe.receiveMessage().sendNextTo ! "a"
      producerProbe.receiveMessage().sendNextTo ! "b"
      producerProbe.receiveMessage().sendNextTo ! "c"
      producerProbe.receiveMessage()

      consumerProbe.receiveMessage().msg should ===("a")

      testKit.stop(producerController)
      producerProbe.expectTerminated(producerController)
      testKit.stop(consumerController)
      consumerProbe.expectTerminated(consumerController)

      val producerController2 = spawn(
        ProducerController[String](
          producerId,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController2 ! ProducerController.Start(producerProbe.ref)

      val consumerController2 = spawn(ConsumerController[String](resendLost = true))
      consumerController2 ! ConsumerController.Start(consumerProbe.ref)
      consumerController2 ! ConsumerController.RegisterToProducerController(producerController2)

      val delivery1 = consumerProbe.receiveMessage()
      delivery1.msg should ===("a")
      delivery1.confirmTo ! ConsumerController.Confirmed(delivery1.seqNr)

      val delivery2 = consumerProbe.receiveMessage()
      delivery2.msg should ===("b")
      delivery2.confirmTo ! ConsumerController.Confirmed(delivery2.seqNr)

      val delivery3 = consumerProbe.receiveMessage()
      delivery3.msg should ===("c")
      delivery3.confirmTo ! ConsumerController.Confirmed(delivery3.seqNr)

      val requestNext4 = producerProbe.receiveMessage()
      requestNext4.currentSeqNr should ===(4)
      requestNext4.sendNextTo ! "d"

      val delivery4 = consumerProbe.receiveMessage()
      delivery4.msg should ===("d")
      delivery4.confirmTo ! ConsumerController.Confirmed(delivery4.seqNr)

      testKit.stop(producerController2)
      testKit.stop(consumerController2)
    }

    "deliver messages after producer restart, keeping same ConsumerController" in {
      val producerId = "p2"
      val producerProbe = createTestProbe[ProducerController.RequestNext[String]]()

      val producerController = spawn(
        ProducerController[String](
          producerId,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController ! ProducerController.Start(producerProbe.ref)

      val consumerController = spawn(ConsumerController[String](resendLost = true))
      val consumerProbe = createTestProbe[ConsumerController.Delivery[String]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)
      consumerController ! ConsumerController.RegisterToProducerController(producerController)

      producerProbe.receiveMessage().sendNextTo ! "a"
      producerProbe.receiveMessage().sendNextTo ! "b"
      producerProbe.receiveMessage().sendNextTo ! "c"
      producerProbe.receiveMessage()

      val delivery1 = consumerProbe.receiveMessage()
      delivery1.msg should ===("a")

      testKit.stop(producerController)

      // TODO how should consumer notice that producerController has stopped?
      // Maybe it should just watch it, since it is anyway responsible for RegisterToProducerController
      consumerProbe.expectTerminated(producerController)

      // FIXME write similar test for sharding, where RegisterToProducerController isn't used, but
      // the ConsumerController just receives a new first SeqMsg from new ProducerController.

      val producerController2 = spawn(
        ProducerController[String](
          producerId,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController2 ! ProducerController.Start(producerProbe.ref)
      consumerController ! ConsumerController.RegisterToProducerController(producerController2)

      delivery1.confirmTo ! ConsumerController.Confirmed(delivery1.seqNr)

      val requestNext4 = producerProbe.receiveMessage()
      requestNext4.currentSeqNr should ===(4)
      requestNext4.sendNextTo ! "d"

      // TODO Should we try harder to deduplicate first?
      val redelivery1 = consumerProbe.receiveMessage()
      redelivery1.msg should ===("a")
      redelivery1.confirmTo ! ConsumerController.Confirmed(redelivery1.seqNr)

      producerProbe.receiveMessage().sendNextTo ! "e"

      val redelivery2 = consumerProbe.receiveMessage()
      redelivery2.msg should ===("b")
      redelivery2.confirmTo ! ConsumerController.Confirmed(redelivery2.seqNr)

      val redelivery3 = consumerProbe.receiveMessage()
      redelivery3.msg should ===("c")
      redelivery3.confirmTo ! ConsumerController.Confirmed(redelivery3.seqNr)

      val delivery4 = consumerProbe.receiveMessage()
      delivery4.msg should ===("d")
      delivery4.confirmTo ! ConsumerController.Confirmed(delivery4.seqNr)

      val delivery5 = consumerProbe.receiveMessage()
      delivery5.msg should ===("e")
      delivery5.confirmTo ! ConsumerController.Confirmed(delivery5.seqNr)

      testKit.stop(producerController)
      testKit.stop(consumerController)
    }

  }

}
