/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.delivery

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.Terminated
import akka.actor.typed.internal.delivery.ConsumerController.SequencedMessage
import akka.actor.typed.internal.delivery.SimuatedSharding.ShardingEnvelope
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import org.scalatest.WordSpecLike

object ReliableDeliveryShardingSpec {

  object TestShardingProducer {

    trait Command
    final case class RequestNext(sendToRef: ActorRef[ShardingEnvelope[TestConsumer.Job]]) extends Command

    private final case object Tick extends Command

    def apply(producerController: ActorRef[ShardingProducerController.Start[TestConsumer.Job]]): Behavior[Command] = {
      Behaviors.setup { context =>
        context.setLoggerName("TestShardingProducer")
        val requestNextAdapter: ActorRef[ShardingProducerController.RequestNext[TestConsumer.Job]] =
          context.messageAdapter(req => RequestNext(req.sendNextTo))
        producerController ! ShardingProducerController.Start(requestNextAdapter)

        // simulate fast producer
        Behaviors.withTimers { timers =>
          timers.startTimerWithFixedDelay(Tick, Tick, 20.millis)
          idle(0)
        }
      }
    }

    private def idle(n: Int): Behavior[Command] = {
      Behaviors.receiveMessage {
        case Tick                => Behaviors.same
        case RequestNext(sendTo) => active(n + 1, sendTo)
      }
    }

    private def active(n: Int, sendTo: ActorRef[ShardingEnvelope[TestConsumer.Job]]): Behavior[Command] = {
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case Tick =>
            val msg = s"msg-$n"
            val entityId = s"entity-${n % 3}"
            ctx.log.info2("sent {} to {}", msg, entityId)
            sendTo ! ShardingEnvelope(entityId, TestConsumer.Job(msg))
            idle(n)

          case RequestNext(_) =>
            throw new IllegalStateException("Unexpected RequestNext, already got one.")
        }
      }
    }

  }

  // FIXME some of this should also become part of Akka
  object TestShardingConsumer {
    def apply(delay: FiniteDuration, endSeqNr: Long, endReplyTo: ActorRef[TestConsumer.CollectedProducerIds])
        : Behavior[ConsumerController.SequencedMessage[TestConsumer.Job]] =
      apply(delay, TestConsumer.consumerEndCondition(endSeqNr), endReplyTo)

    def apply(
        delay: FiniteDuration,
        endCondition: TestConsumer.SomeAsyncJob => Boolean,
        endReplyTo: ActorRef[TestConsumer.CollectedProducerIds])
        : Behavior[ConsumerController.SequencedMessage[TestConsumer.Job]] = {
      Behaviors.setup { context =>
        context.setLoggerName("TestShardingConsumer")
        val consumer = context.spawn(TestConsumer(delay, endCondition, endReplyTo), name = "consumer")
        // if consumer terminates this actor will also terminate
        context.watch(consumer)
        active(consumer, Map.empty)
      }
    }

    private def active(
        consumer: ActorRef[TestConsumer.Command],
        controllers: Map[String, ActorRef[ConsumerController.Command[TestConsumer.Job]]])
        : Behavior[ConsumerController.SequencedMessage[TestConsumer.Job]] = {
      Behaviors
        .receive[ConsumerController.SequencedMessage[TestConsumer.Job]] { (ctx, msg) =>
          controllers.get(msg.producerId) match {
            case Some(c) =>
              c ! msg
              Behaviors.same
            case None =>
              val c = ctx.spawn(
                ConsumerController[TestConsumer.Job](resendLost = true),
                s"consumerController-${msg.producerId}")
              // FIXME watch msg.producerController to cleanup terminated producers
              consumer ! TestConsumer.AddConsumerController(c)
              c ! msg
              active(consumer, controllers.updated(msg.producerId, c))
          }
        }
        .receiveSignal {
          case (_, Terminated(_)) =>
            Behaviors.stopped
        }
    }
  }
}

class ReliableDeliveryShardingSpec extends ScalaTestWithActorTestKit with WordSpecLike with LogCapturing {
  import ReliableDeliveryShardingSpec._
  import TestConsumer.defaultConsumerDelay

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  private def producerId: String = s"p-$idCount"

  "ReliableDelivery with sharding" must {

    "illustrate sharding usage" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.CollectedProducerIds]()
      val sharding: ActorRef[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]] =
        spawn(
          SimuatedSharding(_ => TestShardingConsumer(defaultConsumerDelay, 42, consumerEndProbe.ref)),
          s"sharding-${idCount}")

      val shardingController =
        spawn(ShardingProducerController[TestConsumer.Job](producerId, sharding), s"shardingController-${idCount}")
      val producer = spawn(TestShardingProducer(shardingController), name = s"shardingProducer-${idCount}")

      // expecting 3 end messages, one for each entity: "entity-0", "entity-1", "entity-2"
      consumerEndProbe.receiveMessages(3, 5.seconds)

      testKit.stop(producer)
      testKit.stop(shardingController)
      testKit.stop(sharding)
    }

    "illustrate sharding usage with several producers" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.CollectedProducerIds]()
      val sharding: ActorRef[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]] =
        spawn(
          SimuatedSharding(_ => TestShardingConsumer(defaultConsumerDelay, 42, consumerEndProbe.ref)),
          s"sharding-${idCount}")

      val shardingController1 =
        spawn(
          ShardingProducerController[TestConsumer.Job](
            s"p1-${idCount}", // note different producerId
            sharding),
          s"shardingController1-${idCount}")
      val producer1 = spawn(TestShardingProducer(shardingController1), name = s"shardingProducer1-${idCount}")

      val shardingController2 =
        spawn(
          ShardingProducerController[TestConsumer.Job](
            s"p2-${idCount}", // note different producerId
            sharding),
          s"shardingController2-${idCount}")
      val producer2 = spawn(TestShardingProducer(shardingController2), name = s"shardingProducer2-${idCount}")

      // expecting 3 end messages, one for each entity: "entity-0", "entity-1", "entity-2"
      val endMessages = consumerEndProbe.receiveMessages(3, 5.seconds)
      // verify that they received messages from both producers
      endMessages.flatMap(_.producerIds).toSet should ===(
        Set(
          s"p1-${idCount}-entity-0",
          s"p1-${idCount}-entity-1",
          s"p1-${idCount}-entity-2",
          s"p2-${idCount}-entity-0",
          s"p2-${idCount}-entity-1",
          s"p2-${idCount}-entity-2"))

      testKit.stop(producer1)
      testKit.stop(producer2)
      testKit.stop(shardingController1)
      testKit.stop(shardingController2)
      testKit.stop(sharding)
    }

  }

}
