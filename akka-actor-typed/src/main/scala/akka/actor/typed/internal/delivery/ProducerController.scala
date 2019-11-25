/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.delivery

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.internal.delivery.ConsumerController.SequencedMessage
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.actor.typed.scaladsl.LoggerOps
import akka.util.Timeout

// FIXME Scaladoc describes how it works, internally. Rewrite for end user and keep internals as impl notes.

/**
 * The producer will start the flow by sending a [[ProducerController.Start]] message to the `ProducerController` with
 * message adapter reference to convert [[ProducerController.RequestNext]] message.
 * The sends `RequestNext` to the producer, which is then allowed to send one message to the `ProducerController`.
 *
 * The producer and `ProducerController` are supposed to be local so that these messages are fast and not lost.
 *
 * The `ProducerController` sends the first message to the `ConsumerController` without waiting for
 * a `Request` from the `ConsumerController`. The main reason for this is that when used with
 * Cluster Sharding the first message will typically create the `ConsumerController`. It's
 * also a way to connect the ProducerController and ConsumerController in a dynamic way, for
 * example when the ProducerController is replaced.
 *
 * When the first message is received by the `ConsumerController` it sends back the initial `Request`.
 *
 * Apart from the first message the `ProducerController` will not send more messages than requested
 * by the `ConsumerController`.
 *
 * When there is demand from the consumer side the `ProducerController` sends `RequestNext` to the
 * actual producer, which is then allowed to send one more message.
 *
 * Each message is wrapped by the `ProducerController` in [[ConsumerController.SequencedMessage]] with
 * a monotonically increasing sequence number without gaps, starting at 1.
 *
 * The `Request` message also contains a `confirmedSeqNr` that is the acknowledgement
 * from the consumer that it has received and processed all messages up to that sequence number.
 *
 * The `ConsumerController` will send [[ProducerController.Internal.Resend]] if a lost message is detected
 * and then the `ProducerController` will resend all messages from that sequence number. The producer keeps
 * unconfirmed messages in a buffer to be able to resend them. The buffer size is limited
 * by the request window size.
 *
 * The resending is optional, and the `ConsumerController` can be started with `resendLost=false`
 * to ignore lost messages, and then the `ProducerController` will not buffer unconfirmed messages.
 * In that mode it provides only flow control but no reliable delivery.
 */
object ProducerController {

  sealed trait InternalCommand

  sealed trait Command[A] extends InternalCommand

  final case class Start[A](producer: ActorRef[RequestNext[A]]) extends Command[A]

  final case class RequestNext[A](
      producerId: String,
      currentSeqNr: Long,
      confirmedSeqNr: Long,
      sendNextTo: ActorRef[A],
      askNextTo: ActorRef[MessageWithConfirmation[A]])

  final case class RegisterConsumer[A](consumerController: ActorRef[ConsumerController.Command[A]]) extends Command[A]

  /**
   * For sending confirmation message back to the producer when the message has been fully delivered, processed,
   * and confirmed by the consumer. Typically used with `ask` from the producer.
   */
  final case class MessageWithConfirmation[A](message: A, replyTo: ActorRef[Long]) extends InternalCommand

  object Internal {
    final case class Request(confirmedSeqNr: Long, upToSeqNr: Long, supportResend: Boolean, viaTimeout: Boolean)
        extends InternalCommand {
      require(confirmedSeqNr < upToSeqNr)
    }
    final case class Resend(fromSeqNr: Long) extends InternalCommand
    final case class Ack(confirmedSeqNr: Long) extends InternalCommand
  }

  private case class Msg[A](msg: A) extends InternalCommand
  private case object ResendFirst extends InternalCommand

  private case class LoadStateReply[A](state: DurableProducerState.State[A]) extends InternalCommand
  private case class StoreMessageSentReply(ack: DurableProducerState.StoreMessageSentAck)

  private case class StoreMessageSentCompleted[A](messageSent: DurableProducerState.MessageSent[A])
      extends InternalCommand

  private final case class State[A](
      requested: Boolean,
      currentSeqNr: Long,
      confirmedSeqNr: Long,
      requestedSeqNr: Long,
      pendingReplies: Map[Long, ActorRef[Long]],
      supportResend: Boolean,
      unconfirmed: Vector[ConsumerController.SequencedMessage[A]],
      firstSeqNr: Long,
      producer: ActorRef[ProducerController.RequestNext[A]],
      send: ConsumerController.SequencedMessage[A] => Unit)

  def apply[A: ClassTag](
      producerId: String,
      durableStateBehavior: Option[Behavior[DurableProducerState.Command[A]]]): Behavior[Command[A]] = {
    Behaviors
      .setup[InternalCommand] { context =>
        val durableRef = askLoadState(context, durableStateBehavior)
        waitingForStart[A](None, None, createInitialState(durableRef.nonEmpty)) {
          (producer, consumerController, loadedState) =>
            val send: ConsumerController.SequencedMessage[A] => Unit = consumerController ! _
            becomeActive(producerId, durableRef, createState(context.self, producerId, send, producer, loadedState))
        }
      }
      .narrow
  }

  /**
   * For custom `send` function. For example used with Sharding where the message must be wrapped in
   * `ShardingEnvelope(SequencedMessage(msg))`.
   */
  def apply[A: ClassTag](
      producerId: String,
      durableStateBehavior: Option[Behavior[DurableProducerState.Command[A]]],
      send: ConsumerController.SequencedMessage[A] => Unit): Behavior[Command[A]] = {
    Behaviors
      .setup[InternalCommand] { context =>
        val durableRef = askLoadState(context, durableStateBehavior)
        // ConsumerController not used here
        waitingForStart[A](
          None,
          consumerController = Some(context.system.deadLetters),
          createInitialState(durableRef.nonEmpty)) { (producer, _, loadedState) =>
          becomeActive(producerId, durableRef, createState(context.self, producerId, send, producer, loadedState))
        }
      }
      .narrow
  }

  private def askLoadState[A: ClassTag](
      context: ActorContext[InternalCommand],
      durableStateBehavior: Option[Behavior[DurableProducerState.Command[A]]])
      : Option[ActorRef[DurableProducerState.Command[A]]] = {
    implicit val loadTimeout: Timeout = 10.seconds // FIXME config
    durableStateBehavior.map { b =>
      val ref = context.spawn(b, "durable")
      context.ask[DurableProducerState.LoadState[A], DurableProducerState.State[A]](
        ref,
        askReplyTo => DurableProducerState.LoadState[A](askReplyTo)) {
        case Success(s) => LoadStateReply(s)
        case Failure(e) => throw e // FIXME error handling
      }
      ref
    }
  }

  private def createInitialState[A: ClassTag](hasDurableState: Boolean) = {
    if (hasDurableState) None else Some(DurableProducerState.State[A](1L, 0L, Vector.empty))
  }

  private def createState[A: ClassTag](
      self: ActorRef[InternalCommand],
      producerId: String,
      send: SequencedMessage[A] => Unit,
      producer: ActorRef[RequestNext[A]],
      loadedState: DurableProducerState.State[A]): State[A] = {
    val unconfirmed = loadedState.unconfirmed.zipWithIndex.map {
      case (u, i) => SequencedMessage[A](producerId, u.seqNr, u.msg, i == 0, u.ack)(self)
    }
    State(
      requested = false,
      currentSeqNr = loadedState.currentSeqNr,
      confirmedSeqNr = loadedState.confirmedSeqNr,
      requestedSeqNr = 1L,
      pendingReplies = Map.empty,
      supportResend = true,
      unconfirmed = unconfirmed,
      firstSeqNr = if (loadedState.unconfirmed.isEmpty) 1L else loadedState.unconfirmed.head.seqNr, // FIXME what is the correct firstSeqNr when unconfirmed isEmpty?
      producer,
      send)
  }

  private def waitingForStart[A: ClassTag](
      producer: Option[ActorRef[RequestNext[A]]],
      consumerController: Option[ActorRef[ConsumerController.Command[A]]],
      initialState: Option[DurableProducerState.State[A]])(
      thenBecomeActive: (
          ActorRef[RequestNext[A]],
          ActorRef[ConsumerController.Command[A]],
          DurableProducerState.State[A]) => Behavior[InternalCommand]): Behavior[InternalCommand] = {
    Behaviors.receiveMessagePartial[InternalCommand] {
      case RegisterConsumer(c: ActorRef[ConsumerController.Command[A]] @unchecked) =>
        (producer, initialState) match {
          case (Some(p), Some(s)) => thenBecomeActive(p, c, s)
          case (_, _)             => waitingForStart(producer, Some(c), initialState)(thenBecomeActive)
        }
      case start: Start[A] @unchecked =>
        (consumerController, initialState) match {
          case (Some(c), Some(s)) => thenBecomeActive(start.producer, c, s)
          case (_, _)             => waitingForStart(Some(start.producer), consumerController, initialState)(thenBecomeActive)
        }
      case load: LoadStateReply[A] @unchecked =>
        (producer, consumerController) match {
          case (Some(p), Some(c)) => thenBecomeActive(p, c, load.state)
          case (_, _)             => waitingForStart(producer, consumerController, Some(load.state))(thenBecomeActive)
        }
    }
  }

  private def becomeActive[A: ClassTag](
      producerId: String,
      durableRef: Option[ActorRef[DurableProducerState.Command[A]]],
      state: State[A]): Behavior[InternalCommand] = {

    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        val msgAdapter: ActorRef[A] = ctx.messageAdapter(msg => Msg(msg))
        val requested =
          if (state.unconfirmed.isEmpty) {
            state.producer ! RequestNext(producerId, 1L, 0L, msgAdapter, ctx.self)
            true
          } else {
            ctx.self ! ResendFirst
            false
          }
        new ProducerController[A](ctx, producerId, durableRef, msgAdapter, timers)
          .active(state.copy(requested = requested))
      }
    }
  }

}

private class ProducerController[A: ClassTag](
    ctx: ActorContext[ProducerController.InternalCommand],
    producerId: String,
    durableRef: Option[ActorRef[DurableProducerState.Command[A]]],
    msgAdapter: ActorRef[A],
    timers: TimerScheduler[ProducerController.InternalCommand]) {
  import ProducerController._
  import ProducerController.Internal._
  import ConsumerController.SequencedMessage
  import DurableProducerState.StoreMessageSent
  import DurableProducerState.StoreMessageSentAck
  import DurableProducerState.StoreMessageConfirmed
  import DurableProducerState.MessageSent

  private implicit val askTimeout: Timeout = 5.seconds // FIXME config

  private def active(s: State[A]): Behavior[InternalCommand] = {

    def onMsg(m: A, newPendingReplies: Map[Long, ActorRef[Long]], ack: Boolean): Behavior[InternalCommand] = {
      checkOnMsgRequestedState()
      // FIXME adjust all logging, most should probably be debug
      ctx.log.info("sent [{}]", s.currentSeqNr)
      val seqMsg = SequencedMessage(producerId, s.currentSeqNr, m, s.currentSeqNr == s.firstSeqNr, ack)(ctx.self)
      val newUnconfirmed =
        if (s.supportResend) s.unconfirmed :+ seqMsg
        else Vector.empty // no resending, no need to keep unconfirmed

      if (s.currentSeqNr == s.firstSeqNr)
        timers.startTimerWithFixedDelay(ResendFirst, ResendFirst, 1.second)

      s.send(seqMsg)
      val newRequested =
        if (s.currentSeqNr == s.requestedSeqNr)
          false
        else {
          s.producer ! RequestNext(producerId, s.currentSeqNr + 1, s.confirmedSeqNr, msgAdapter, ctx.self)
          true
        }
      active(
        s.copy(
          requested = newRequested,
          currentSeqNr = s.currentSeqNr + 1,
          pendingReplies = newPendingReplies,
          unconfirmed = newUnconfirmed))
    }

    def checkOnMsgRequestedState(): Unit = {
      if (!s.requested || s.currentSeqNr > s.requestedSeqNr) {
        throw new IllegalStateException(
          s"Unexpected Msg when no demand, requested ${s.requested}, " +
          s"requestedSeqNr ${s.requestedSeqNr}, currentSeqNr ${s.currentSeqNr}")
      }
    }

    def storeMessageSent(m: A, ack: Boolean): Unit = {
      val messageSent = MessageSent(s.currentSeqNr, m, ack)
      ctx.ask[StoreMessageSent[A], StoreMessageSentAck](
        durableRef.get,
        askReplyTo => StoreMessageSent(MessageSent(s.currentSeqNr, m, ack), askReplyTo)) {
        case Success(_) => StoreMessageSentCompleted(messageSent)
        case Failure(e) => throw e // FIXME error handling
      }
    }

    def onAck(newConfirmedSeqNr: Long): State[A] = {
      // FIXME use more efficient Map for pendingReplies, sorted, maybe just `Vector[(Long, ActorRef)]`
      val newPendingReplies =
        if (s.pendingReplies.isEmpty)
          s.pendingReplies
        else {
          val replies = s.pendingReplies.keys.filter(_ <= newConfirmedSeqNr).toVector.sorted
          if (replies.nonEmpty)
            ctx.log.info("Confirmation replies from [{}] to [{}]", replies.min, replies.max)
          replies.foreach { seqNr =>
            s.pendingReplies(seqNr) ! seqNr
          }
          s.pendingReplies -- replies
        }

      val newUnconfirmed =
        if (s.supportResend) s.unconfirmed.dropWhile(_.seqNr <= newConfirmedSeqNr)
        else Vector.empty

      if (newConfirmedSeqNr == s.firstSeqNr)
        timers.cancel(ResendFirst)

      val newMaxConfirmedSeqNr = math.max(s.confirmedSeqNr, newConfirmedSeqNr)

      durableRef.foreach { d =>
        // Storing the confirmedSeqNr can be "write behind", at-least-once delivery
        // FIXME to reduce number of writes we could consider to only StoreMessageConfirmed for the Request messages and not for each Ack
        if (newMaxConfirmedSeqNr != s.confirmedSeqNr)
          d ! StoreMessageConfirmed(newMaxConfirmedSeqNr)
      }

      s.copy(confirmedSeqNr = newMaxConfirmedSeqNr, pendingReplies = newPendingReplies, unconfirmed = newUnconfirmed)
    }

    def resendUnconfirmed(newUnconfirmed: Vector[SequencedMessage[A]]): Unit = {
      if (newUnconfirmed.nonEmpty)
        ctx.log.info("resending [{} - {}]", newUnconfirmed.head.seqNr, newUnconfirmed.last.seqNr)
      newUnconfirmed.foreach(s.send)
    }

    Behaviors.receiveMessage {
      case MessageWithConfirmation(m: A, replyTo) =>
        val newPendingReplies = s.pendingReplies.updated(s.currentSeqNr, replyTo)
        if (durableRef.isEmpty) {
          onMsg(m, newPendingReplies, ack = true)
        } else {
          storeMessageSent(m, ack = true)
          active(s.copy(pendingReplies = newPendingReplies))
        }

      case Msg(m: A) =>
        if (durableRef.isEmpty) {
          onMsg(m, s.pendingReplies, ack = false)
        } else {
          storeMessageSent(m, ack = false)
          Behaviors.same
        }

      case StoreMessageSentCompleted(MessageSent(seqNr, m: A, ack)) =>
        if (seqNr != s.currentSeqNr)
          throw new IllegalStateException(s"currentSeqNr [${s.currentSeqNr}] not matching stored seqNr [$seqNr]")
        onMsg(m, s.pendingReplies, ack)

      case Request(newConfirmedSeqNr, newRequestedSeqNr, supportResend, viaTimeout) =>
        ctx.log.infoN(
          "Request, confirmed [{}], requested [{}], current [{}]",
          newConfirmedSeqNr,
          newRequestedSeqNr,
          s.currentSeqNr)

        val stateAfterAck = onAck(newConfirmedSeqNr)

        val newUnconfirmed =
          if (supportResend) stateAfterAck.unconfirmed
          else Vector.empty

        if ((viaTimeout || newConfirmedSeqNr == s.firstSeqNr) && supportResend) {
          // the last message was lost and no more message was sent that would trigger Resend
          resendUnconfirmed(newUnconfirmed)
        }

        if (newRequestedSeqNr > s.requestedSeqNr) {
          if (!s.requested && (newRequestedSeqNr - s.currentSeqNr) > 0)
            s.producer ! RequestNext(producerId, s.currentSeqNr, newConfirmedSeqNr, msgAdapter, ctx.self)
          active(
            stateAfterAck.copy(
              requested = true,
              requestedSeqNr = newRequestedSeqNr,
              supportResend = supportResend,
              unconfirmed = newUnconfirmed))
        } else {
          active(stateAfterAck.copy(supportResend = supportResend, unconfirmed = newUnconfirmed))
        }

      case Ack(newConfirmedSeqNr) =>
        ctx.log.infoN("Ack, confirmed [{}], current [{}]", newConfirmedSeqNr, s.currentSeqNr)
        val stateAfterAck = onAck(newConfirmedSeqNr)
        if (newConfirmedSeqNr == s.firstSeqNr && stateAfterAck.unconfirmed.nonEmpty) {
          resendUnconfirmed(stateAfterAck.unconfirmed)
        }
        active(stateAfterAck)

      case Resend(fromSeqNr) =>
        val newUnconfirmed = s.unconfirmed.dropWhile(_.seqNr < fromSeqNr)
        resendUnconfirmed(newUnconfirmed)
        active(s.copy(unconfirmed = newUnconfirmed))

      case ResendFirst =>
        if (s.unconfirmed.nonEmpty && s.unconfirmed.head.seqNr == s.firstSeqNr) {
          ctx.log.info("resending first, [{}]", s.firstSeqNr)
          s.send(s.unconfirmed.head.copy(first = true)(ctx.self))
        } else {
          if (s.currentSeqNr > s.firstSeqNr)
            timers.cancel(ResendFirst)
        }
        Behaviors.same

      case start: Start[A] @unchecked =>
        ctx.log.info("Register new Producer [{}], currentSeqNr [{}].", start.producer, s.currentSeqNr)
        if (s.requested)
          start.producer ! RequestNext(producerId, s.currentSeqNr, s.confirmedSeqNr, msgAdapter, ctx.self)
        active(s.copy(producer = start.producer))

      case RegisterConsumer(consumerController: ActorRef[ConsumerController.Command[A]] @unchecked) =>
        val newFirstSeqNr =
          if (s.unconfirmed.isEmpty) s.currentSeqNr
          else s.unconfirmed.head.seqNr
        ctx.log.info(
          "Register new ConsumerController [{}], starting with seqNr [{}].",
          consumerController,
          newFirstSeqNr)
        if (s.unconfirmed.nonEmpty) {
          timers.startTimerWithFixedDelay(ResendFirst, ResendFirst, 1.second)
          ctx.self ! ResendFirst
        }
        // update the send function
        val newSend = consumerController ! _
        active(s.copy(firstSeqNr = newFirstSeqNr, send = newSend))
    }
  }
}

// FIXME there should also be a durable version of this (using EventSouredBehavior) that stores the
// unconfirmed messages before sending and stores ack event when confirmed.
