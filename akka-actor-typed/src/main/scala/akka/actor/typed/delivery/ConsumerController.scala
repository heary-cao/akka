/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.StashBuffer
import akka.actor.typed.scaladsl.TimerScheduler
import akka.annotation.InternalApi

// FIXME Scaladoc describes how it works, internally. Rewrite for end user and keep internals as impl notes.

/**
 * The destination consumer will start the flow by sending an initial `Start` message
 * to the `ConsumerController`.
 *
 * The `ProducerController` sends the first message to the `ConsumerController` without waiting for
 * a `Request` from the `ConsumerController`. The main reason for this is that when used with
 * Cluster Sharding the first message will typically create the `ConsumerController`. It's
 * also a way to connect the ProducerController and ConsumerController in a dynamic way, for
 * example when the ProducerController is replaced.
 *
 * The `ConsumerController` sends [[ProducerController.Internal.Request]] to the `ProducerController`
 * to specify it's ready to receive up to the requested sequence number.
 *
 * The `ConsumerController` sends the first `Request` when it receives the first `SequencedMessage`
 * and has received the `Start` message from the consumer.
 *
 * It sends new `Request` when half of the requested window is remaining, but it also retries
 * the `Request` if no messages are received because that could be caused by lost messages.
 *
 * Apart from the first message the producer will not send more messages than requested.
 *
 * Received messages are wrapped in [[ConsumerController.Delivery]] when sent to the consumer,
 * which is supposed to reply with [[ConsumerController.Confirmed]] when it has processed the message.
 * Next message is not delivered until the previous is confirmed.
 * More messages from the producer that arrive while waiting for the confirmation are stashed by
 * the `ConsumerController` and delivered when previous message was confirmed.
 *
 * In other words, the "request" protocol to the application producer and consumer is one-by-one, but
 * between the `ProducerController` and `ConsumerController` it's window of messages in flight.
 *
 * The consumer and the `ConsumerController` are supposed to be local so that these messages are fast and not lost.
 *
 * If the `ConsumerController` receives a message with unexpected sequence number (not previous + 1)
 * it sends [[ProducerController.Internal.Resend]] to the `ProducerController` and will ignore all messages until
 * the expected sequence number arrives.
 */
object ConsumerController {

  type SeqNr = Long

  sealed trait InternalCommand
  sealed trait Command[+A] extends InternalCommand
  final case class SequencedMessage[A](producerId: String, seqNr: SeqNr, msg: A, first: Boolean, ack: Boolean)(
      /** INTERNAL API */
      @InternalApi private[akka] val producer: ActorRef[ProducerController.InternalCommand])
      extends Command[A]
  private final case object Retry extends InternalCommand

  final case class Delivery[A](producerId: String, seqNr: SeqNr, msg: A, confirmTo: ActorRef[Confirmed])
  final case class Start[A](deliverTo: ActorRef[Delivery[A]]) extends Command[A]
  final case class Confirmed(seqNr: SeqNr) extends InternalCommand

  final case class RegisterToProducerController[A](producerController: ActorRef[ProducerController.Command[A]])
      extends Command[A]

  private final case class ConsumerTerminated(consumer: ActorRef[_]) extends InternalCommand

  private final case class State[A](
      producer: ActorRef[ProducerController.InternalCommand],
      consumer: ActorRef[ConsumerController.Delivery[A]],
      receivedSeqNr: SeqNr,
      confirmedSeqNr: SeqNr,
      requestedSeqNr: SeqNr,
      registering: Option[ActorRef[ProducerController.Command[A]]])

  private val RequestWindow = 20 // FIXME should be a param, ofc

  def apply[A](): Behavior[Command[A]] =
    apply(resendLost = true, serviceKey = None)

  /**
   * Lost messages will not be resent, but flow control is used.
   * This can be more efficient since messages doesn't have to be
   * kept in memory in the `ProducerController` until they have been
   * confirmed.
   */
  def onlyFlowControl[A](): Behavior[Command[A]] =
    apply(resendLost = false, serviceKey = None)

  /**
   * To be used with [[WorkPullingProducerController]]. It will register itself to the
   * [[Receptionist]] with the given `serviceKey`, and the `WorkPullingProducerController`
   * subscribes to the same key to find active workers.
   */
  def apply[A](serviceKey: ServiceKey[Command[A]]): Behavior[Command[A]] =
    apply(resendLost = true, Some(serviceKey))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def apply[A](
      resendLost: Boolean,
      serviceKey: Option[ServiceKey[Command[A]]]): Behavior[Command[A]] = {
    Behaviors
      .withStash[InternalCommand](RequestWindow) { stashBuffer =>
        Behaviors.setup { ctx =>
          ctx.setLoggerName(classOf[ConsumerController[_]])
          serviceKey.foreach { key =>
            ctx.system.receptionist ! Receptionist.Register(key, ctx.self)
          }
          Behaviors.withTimers { timers =>
            def becomeActive(
                producerId: String,
                producer: ActorRef[ProducerController.InternalCommand],
                start: Start[A],
                firstSeqNr: SeqNr): Behavior[InternalCommand] = {
              val requestedSeqNr = firstSeqNr - 1 + RequestWindow

              // FIXME adjust all logging, most should probably be debug
              ctx.log.infoN(
                "Become active for producer [{}] / [{}], requestedSeqNr [{}]",
                producerId,
                producer,
                requestedSeqNr)

              producer ! ProducerController.Internal.Request(
                confirmedSeqNr = 0,
                requestedSeqNr,
                resendLost,
                viaTimeout = false)

              val next = new ConsumerController[A](ctx, timers, stashBuffer, resendLost).active(
                State(
                  producer,
                  start.deliverTo,
                  receivedSeqNr = 0,
                  confirmedSeqNr = 0,
                  requestedSeqNr,
                  registering = None))
              if (stashBuffer.nonEmpty)
                ctx.log.info("Unstash [{}]", stashBuffer.size)
              stashBuffer.unstashAll(next)
            }

            // wait for both the `Start` message from the consumer and the first `SequencedMessage` from the producer
            def idle(
                register: Option[ActorRef[ProducerController.Command[A]]],
                producer: Option[(String, ActorRef[ProducerController.InternalCommand])],
                start: Option[Start[A]],
                firstSeqNr: SeqNr): Behavior[InternalCommand] = {
              Behaviors.receiveMessagePartial {
                case reg: RegisterToProducerController[A] @unchecked =>
                  reg.producerController ! ProducerController.RegisterConsumer(ctx.self)
                  idle(Some(reg.producerController), producer = None, start, firstSeqNr)

                case s: Start[A] @unchecked =>
                  start.foreach(previous => ctx.unwatch(previous.deliverTo))
                  ctx.watchWith(s.deliverTo, ConsumerTerminated(s.deliverTo))
                  producer match {
                    case None           => idle(register, None, Some(s), firstSeqNr)
                    case Some((pid, p)) => becomeActive(pid, p, s, firstSeqNr)

                  }

                case seqMsg: SequencedMessage[A] @unchecked =>
                  if (seqMsg.first) {
                    ctx.log.info("Received first SequencedMessage [{}]", seqMsg.seqNr)
                    stashBuffer.stash(seqMsg)
                    start match {
                      case None    => idle(None, Some((seqMsg.producerId, seqMsg.producer)), start, seqMsg.seqNr)
                      case Some(s) => becomeActive(seqMsg.producerId, seqMsg.producer, s, seqMsg.seqNr)
                    }
                  } else if (seqMsg.seqNr > firstSeqNr) {
                    ctx.log.info("Stashing non-first SequencedMessage [{}]", seqMsg.seqNr)
                    stashBuffer.stash(seqMsg)
                    Behaviors.same
                  } else {
                    ctx.log.info("Dropping non-first SequencedMessage [{}]", seqMsg.seqNr)
                    Behaviors.same
                  }

                case Retry =>
                  register.foreach { reg =>
                    ctx.log.info("retry RegisterConsumer to [{}]", reg)
                    reg ! ProducerController.RegisterConsumer(ctx.self)
                  }
                  Behaviors.same

                case ConsumerTerminated(c) =>
                  ctx.log.info("Consumer [{}] terminated", c)
                  Behaviors.stopped

              }

            }

            timers.startTimerWithFixedDelay(Retry, Retry, 1.second) // FIXME config interval
            idle(None, None, None, 1L)
          }
        }
      }
      .narrow // expose Command, but not InternalCommand
  }

}

private class ConsumerController[A](
    context: ActorContext[ConsumerController.InternalCommand],
    timers: TimerScheduler[ConsumerController.InternalCommand],
    stashBuffer: StashBuffer[ConsumerController.InternalCommand],
    resendLost: Boolean) {

  startRetryTimer()

  import ConsumerController._
  import ProducerController.Internal.Request
  import ProducerController.Internal.Ack
  import ProducerController.Internal.Resend

  // Expecting a SequencedMessage from ProducerController, that will be delivered to the consumer if
  // the seqNr is right.
  private def active(s: State[A]): Behavior[InternalCommand] = {
    Behaviors.receiveMessage {
      case seqMsg: SequencedMessage[A] =>
        val pid = seqMsg.producerId
        val seqNr = seqMsg.seqNr
        val expectedSeqNr = s.receivedSeqNr + 1

        // FIXME, first is resent. How to avoid delivering duplicate first? Need epoch id?

        if (s.registering.isDefined && !seqMsg.first) {
          context.log.infoN(
            "from producer [{}], discarding message because registering to new ProducerController, seqNr [{}]",
            pid,
            seqNr)
          Behaviors.same
        } else if (seqMsg.producer != s.producer && !seqMsg.first) {
          context.log.infoN(
            "from producer [{}], discarding message because unexpected producer ActorRef, seqNr [{}]",
            pid,
            seqNr)
          Behaviors.same
        } else if (isExpected(s, seqMsg)) {
          logIfChangingProducer(s.producer, seqMsg, pid, seqNr)
          context.log.info("from producer [{}], deliver [{}] to consumer", pid, seqNr)
          s.consumer ! Delivery(pid, seqNr, seqMsg.msg, context.self)
          waitingForConfirmation(
            s.copy(
              producer = seqMsg.producer,
              receivedSeqNr = seqNr,
              requestedSeqNr = s.requestedSeqNr,
              registering = updatedRegistering(s, seqMsg)),
            seqMsg)
        } else if (seqNr > expectedSeqNr) {
          logIfChangingProducer(s.producer, seqMsg, pid, seqNr)
          context.log.infoN("from producer [{}], missing [{}], received [{}]", pid, expectedSeqNr, seqNr)
          if (resendLost) {
            seqMsg.producer ! Resend(fromSeqNr = expectedSeqNr)
            resending(s.copy(producer = seqMsg.producer, registering = updatedRegistering(s, seqMsg)))
          } else {
            context.log.info("from producer [{}], deliver [{}] to consumer", pid, seqNr)
            s.consumer ! Delivery(pid, seqNr, seqMsg.msg, context.self)
            waitingForConfirmation(
              s.copy(producer = seqMsg.producer, receivedSeqNr = seqNr, registering = updatedRegistering(s, seqMsg)),
              seqMsg)
          }
        } else { // seqNr < expectedSeqNr
          context.log.infoN("from producer [{}], deduplicate [{}], expected [{}]", pid, seqNr, expectedSeqNr)
          if (seqMsg.first)
            active(retryRequest(s).copy(registering = updatedRegistering(s, seqMsg)))
          else
            active(s.copy(registering = updatedRegistering(s, seqMsg)))
        }

      case Retry =>
        s.registering match {
          case None =>
            active(retryRequest(s))
          case Some(reg) =>
            reg ! ProducerController.RegisterConsumer(context.self)
            Behaviors.same
        }

      case Confirmed(seqNr) =>
        context.log.warn("Unexpected confirmed [{}]", seqNr)
        Behaviors.unhandled

      case start: Start[A] =>
        // if consumer is restarted it may send Start again
        context.unwatch(s.consumer)
        context.watchWith(start.deliverTo, ConsumerTerminated(start.deliverTo))
        active(s.copy(consumer = start.deliverTo))

      case ConsumerTerminated(c) =>
        context.log.info("Consumer [{}] terminated", c)
        Behaviors.stopped

      case reg: RegisterToProducerController[A] =>
        if (reg.producerController != s.producer) {
          context.log.info2(
            "Register to new ProducerController [{}], previous was [{}]",
            reg.producerController,
            s.producer)
          reg.producerController ! ProducerController.RegisterConsumer(context.self)
          active(s.copy(registering = Some(reg.producerController)))
        } else {
          Behaviors.same
        }
    }
  }

  private def isExpected(s: State[A], seqMsg: SequencedMessage[A]): Boolean = {
    val expectedSeqNr = s.receivedSeqNr + 1
    seqMsg.seqNr == expectedSeqNr ||
    (seqMsg.first && seqMsg.seqNr >= expectedSeqNr) ||
    (seqMsg.first && seqMsg.producer != s.producer)
  }

  private def updatedRegistering(
      s: State[A],
      seqMsg: SequencedMessage[A]): Option[ActorRef[ProducerController.Command[A]]] = {
    s.registering match {
      case None          => None
      case s @ Some(reg) => if (seqMsg.producer == reg) None else s
    }
  }

  // It has detected a missing seqNr and requested a Resend. Expecting a SequencedMessage from the
  // ProducerController with the missing seqNr. Other SequencedMessage with different seqNr will be
  // discarded since they were in flight before the Resend request and will anyway be sent again.
  private def resending(s: State[A]): Behavior[InternalCommand] = {
    Behaviors.receiveMessage {
      case seqMsg: SequencedMessage[A] =>
        val pid = seqMsg.producerId
        val seqNr = seqMsg.seqNr

        if (s.registering.isDefined && !seqMsg.first) {
          context.log.infoN(
            "from producer [{}], discarding message because registering to new ProducerController, seqNr [{}]",
            pid,
            seqNr)
          Behaviors.same
        } else if (seqMsg.producer != s.producer && !seqMsg.first) {
          context.log.infoN(
            "from producer [{}], discarding message because unexpected producer ActorRef, seqNr [{}]",
            pid,
            seqNr)
          Behaviors.same
        } else if (isExpected(s, seqMsg)) {
          logIfChangingProducer(s.producer, seqMsg, pid, seqNr)
          context.log.infoN(
            "from producer [{}], received {} [{}]",
            pid,
            if (seqMsg.first) "first" else "missing",
            seqNr)
          context.log.info("from producer [{}], deliver [{}] to consumer", pid, seqNr)
          s.consumer ! Delivery(pid, seqNr, seqMsg.msg, context.self)
          waitingForConfirmation(
            s.copy(producer = seqMsg.producer, receivedSeqNr = seqNr, registering = updatedRegistering(s, seqMsg)),
            seqMsg)
        } else {
          context.log.infoN("from producer [{}], ignoring [{}], waiting for [{}]", pid, seqNr, s.receivedSeqNr + 1)
          if (seqMsg.first)
            retryRequest(s)
          Behaviors.same // ignore until we receive the expected
        }

      case Retry =>
        s.registering match {
          case None =>
            // in case the Resend message was lost
            context.log.info("retry Resend [{}]", s.receivedSeqNr + 1)
            s.producer ! Resend(fromSeqNr = s.receivedSeqNr + 1)
            Behaviors.same
          case Some(reg) =>
            reg ! ProducerController.RegisterConsumer(context.self)
            Behaviors.same
        }

      case Confirmed(seqNr) =>
        context.log.warn("Unexpected confirmed [{}]", seqNr)
        Behaviors.unhandled

      case start: Start[A] =>
        // if consumer is restarted it may send Start again
        context.unwatch(s.consumer)
        context.watchWith(start.deliverTo, ConsumerTerminated(start.deliverTo))
        resending(s.copy(consumer = start.deliverTo))

      case ConsumerTerminated(c) =>
        context.log.info("Consumer [{}] terminated", c)
        Behaviors.stopped

      case reg: RegisterToProducerController[A] =>
        if (reg.producerController != s.producer) {
          context.log.info2(
            "Register to new ProducerController [{}], previous was [{}]",
            reg.producerController,
            s.producer)
          reg.producerController ! ProducerController.RegisterConsumer(context.self)
          resending(s.copy(registering = Some(reg.producerController)))
        } else {
          Behaviors.same
        }
    }
  }

  // The message has been delivered to the consumer and it is now waiting for Confirmed from
  // the consumer. New SequencedMessage from the ProducerController will be stashed.
  private def waitingForConfirmation(s: State[A], seqMsg: SequencedMessage[A]): Behavior[InternalCommand] = {
    Behaviors.receiveMessage {
      case Confirmed(seqNr) =>
        val expectedSeqNr = s.receivedSeqNr
        if (seqNr > expectedSeqNr) {
          throw new IllegalStateException(
            s"Expected confirmation of seqNr [$expectedSeqNr], but received higher [$seqNr]")
        } else if (seqNr != expectedSeqNr) {
          context.log.info(
            "Expected confirmation of seqNr [{}] but received [{}]. Perhaps the consumer was restarted.",
            expectedSeqNr,
            seqNr)
        }
        context.log.info("Confirmed [{}], stashed size [{}]", seqNr, stashBuffer.size)

        val newRequestedSeqNr =
          if (seqMsg.first) {
            // confirm the first message immediately to cancel resending of first
            val newRequestedSeqNr = seqNr - 1 + RequestWindow
            context.log.info("Request after first [{}]", newRequestedSeqNr)
            s.producer ! Request(confirmedSeqNr = seqNr, newRequestedSeqNr, resendLost, viaTimeout = false)
            newRequestedSeqNr
          } else if ((s.requestedSeqNr - seqNr) == RequestWindow / 2) {
            val newRequestedSeqNr = s.requestedSeqNr + RequestWindow / 2
            context.log.info("Request [{}]", newRequestedSeqNr)
            s.producer ! Request(confirmedSeqNr = seqNr, newRequestedSeqNr, resendLost, viaTimeout = false)
            startRetryTimer() // reset interval since Request was just sent
            newRequestedSeqNr
          } else {
            if (seqMsg.ack) {
              context.log.info("Ack [{}]", seqNr)
              s.producer ! Ack(confirmedSeqNr = seqNr)
            }

            s.requestedSeqNr
          }

        // FIXME can we use unstashOne instead of all?
        if (stashBuffer.nonEmpty)
          context.log.info("Unstash [{}]", stashBuffer.size)
        stashBuffer.unstashAll(active(s.copy(confirmedSeqNr = seqNr, requestedSeqNr = newRequestedSeqNr)))

      case seqMsg: SequencedMessage[A] =>
        if (stashBuffer.isFull) {
          // possible that the stash is full if ProducerController resends unconfirmed (duplicates)
          // dropping them since they can be resent
          context.log.info("Stash is full, dropping [{}]", seqMsg)
        } else {
          context.log.info("Stash [{}]", seqMsg)
          stashBuffer.stash(seqMsg)
        }
        Behaviors.same

      case Retry =>
        s.registering match {
          case None => waitingForConfirmation(retryRequest(s), seqMsg)
          case Some(reg) =>
            reg ! ProducerController.RegisterConsumer(context.self)
            Behaviors.same
        }

      case start: Start[A] =>
        // if consumer is restarted it may send Start again
        context.unwatch(s.consumer)
        context.watchWith(start.deliverTo, ConsumerTerminated(start.deliverTo))
        context.log.info("from producer [{}], deliver [{}] to consumer, after Start", seqMsg.producerId, seqMsg.seqNr)
        start.deliverTo ! Delivery(seqMsg.producerId, seqMsg.seqNr, seqMsg.msg, context.self)
        waitingForConfirmation(s.copy(consumer = start.deliverTo), seqMsg)

      case ConsumerTerminated(c) =>
        context.log.info("Consumer [{}] terminated", c)
        Behaviors.stopped

      case reg: RegisterToProducerController[A] =>
        if (reg.producerController != s.producer) {
          context.log.info2(
            "Register to new ProducerController [{}], previous was [{}]",
            reg.producerController,
            s.producer)
          reg.producerController ! ProducerController.RegisterConsumer(context.self)
          resending(s.copy(registering = Some(reg.producerController)))
          waitingForConfirmation(s.copy(registering = Some(reg.producerController)), seqMsg)
        } else {
          Behaviors.same
        }
    }
  }

  private def startRetryTimer(): Unit = {
    timers.startTimerWithFixedDelay(Retry, Retry, 1.second) // FIXME config interval
  }

  // in case the Request or the SequencedMessage triggering the Request is lost
  private def retryRequest(s: State[A]): State[A] = {
    val newRequestedSeqNr = if (resendLost) s.requestedSeqNr else s.receivedSeqNr + RequestWindow / 2
    context.log.info("retry Request [{}]", newRequestedSeqNr)
    // FIXME may watch the producer to avoid sending retry Request to dead producer
    s.producer ! Request(s.confirmedSeqNr, newRequestedSeqNr, resendLost, viaTimeout = true)
    s.copy(requestedSeqNr = newRequestedSeqNr)
  }

  private def logIfChangingProducer(
      producer: ActorRef[ProducerController.InternalCommand],
      seqMsg: SequencedMessage[A],
      producerId: String,
      seqNr: SeqNr): Unit = {
    if (seqMsg.producer != producer)
      context.log.infoN(
        "changing producer [{}] from [{}] to [{}], seqNr [{}]",
        producerId,
        producer,
        seqMsg.producer,
        seqNr)
  }

}
