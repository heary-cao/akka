/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import ConsumerController.SequencedMessage
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.actor.typed.scaladsl.LoggerOps
import akka.util.Timeout

// FIXME Scaladoc describes how it works, internally. Rewrite for end user and keep internals as impl notes.

/**
 * The producer will start the flow by sending a [[ProducerController.Start]] message to the `ProducerController` with
 * message adapter reference to convert [[ProducerController.RequestNext]] message.
 * The `ProducerController` sends `RequestNext` to the producer, which is then allowed to send one message to
 * the `ProducerController`.
 *
 * The producer and `ProducerController` are supposed to be local so that these messages are fast and not lost.
 *
 * The `ProducerController` sends the first message to the `ConsumerController` without waiting for
 * a `Request` from the `ConsumerController`. The main reason for this is that when used with
 * Cluster Sharding the first message will typically create the `ConsumerController`. It's
 * also a way to connect the ProducerController and ConsumerController in a dynamic way, for
 * example when the ProducerController is replaced.
 *
 * When the first message is received by the `ConsumerController` it sends back the initial `Request`,
 * with demand of how many messages it can accept.
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
 * In other words, the "request" protocol to the application producer and consumer is one-by-one, but
 * between the `ProducerController` and `ConsumerController` it's window of messages in flight.
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

  type SeqNr = Long

  sealed trait InternalCommand

  sealed trait Command[A] extends InternalCommand

  final case class Start[A](producer: ActorRef[RequestNext[A]]) extends Command[A]

  final case class RequestNext[A](
      producerId: String,
      currentSeqNr: SeqNr,
      confirmedSeqNr: SeqNr,
      sendNextTo: ActorRef[A],
      askNextTo: ActorRef[MessageWithConfirmation[A]])

  final case class RegisterConsumer[A](consumerController: ActorRef[ConsumerController.Command[A]]) extends Command[A]

  /**
   * For sending confirmation message back to the producer when the message has been confirmed.
   * Typically used with `ask` from the producer.
   *
   * If `DurableProducerQueue` is used the confirmation reply is sent when the message has been
   * successfully stored, meaning that the actual delivery to the consumer may happen later.
   * If `DurableProducerQueue` is not used the confirmation reply is sent when the message has been
   * fully delivered, processed, and confirmed by the consumer.
   */
  final case class MessageWithConfirmation[A](message: A, replyTo: ActorRef[SeqNr]) extends InternalCommand

  object Internal {
    final case class Request(confirmedSeqNr: SeqNr, upToSeqNr: SeqNr, supportResend: Boolean, viaTimeout: Boolean)
        extends InternalCommand {
      require(confirmedSeqNr <= upToSeqNr, s"confirmedSeqNr [$confirmedSeqNr] should be <= upToSeqNr [$upToSeqNr]")
    }
    final case class Resend(fromSeqNr: SeqNr) extends InternalCommand
    final case class Ack(confirmedSeqNr: SeqNr) extends InternalCommand
  }

  private case class Msg[A](msg: A) extends InternalCommand
  private case object ResendFirst extends InternalCommand

  private case class LoadStateReply[A](state: DurableProducerQueue.State[A]) extends InternalCommand
  private case class LoadStateFailed(attempt: Int) extends InternalCommand
  private case class StoreMessageSentReply(ack: DurableProducerQueue.StoreMessageSentAck)
  private case class StoreMessageSentFailed[A](messageSent: DurableProducerQueue.MessageSent[A], attempt: Int)
      extends InternalCommand

  private case class StoreMessageSentCompleted[A](messageSent: DurableProducerQueue.MessageSent[A])
      extends InternalCommand

  private final case class State[A](
      requested: Boolean,
      currentSeqNr: SeqNr,
      confirmedSeqNr: SeqNr,
      requestedSeqNr: SeqNr,
      replyAfterStore: Map[SeqNr, ActorRef[SeqNr]],
      supportResend: Boolean,
      unconfirmed: Vector[ConsumerController.SequencedMessage[A]],
      firstSeqNr: SeqNr,
      producer: ActorRef[ProducerController.RequestNext[A]],
      send: ConsumerController.SequencedMessage[A] => Unit)

  def apply[A: ClassTag](
      producerId: String,
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]]): Behavior[Command[A]] = {
    Behaviors
      .setup[InternalCommand] { context =>
        context.setLoggerName(classOf[ProducerController[_]])
        val durableQueue = askLoadState(context, durableQueueBehavior)
        waitingForStart[A](context, None, None, durableQueue, createInitialState(durableQueue.nonEmpty)) {
          (producer, consumerController, loadedState) =>
            val send: ConsumerController.SequencedMessage[A] => Unit = consumerController ! _
            becomeActive(producerId, durableQueue, createState(context.self, producerId, send, producer, loadedState))
        }
      }
      .narrow
  }

  // FIXME javadsl create methods

  /**
   * For custom `send` function. For example used with Sharding where the message must be wrapped in
   * `ShardingEnvelope(SequencedMessage(msg))`.
   */
  def apply[A: ClassTag](
      producerId: String,
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      send: ConsumerController.SequencedMessage[A] => Unit): Behavior[Command[A]] = {
    Behaviors
      .setup[InternalCommand] { context =>
        context.setLoggerName(classOf[ProducerController[_]])
        val durableQueue = askLoadState(context, durableQueueBehavior)
        // ConsumerController not used here
        waitingForStart[A](
          context,
          None,
          consumerController = Some(context.system.deadLetters),
          durableQueue,
          createInitialState(durableQueue.nonEmpty)) { (producer, _, loadedState) =>
          becomeActive(producerId, durableQueue, createState(context.self, producerId, send, producer, loadedState))
        }
      }
      .narrow
  }

  private def askLoadState[A: ClassTag](
      context: ActorContext[InternalCommand],
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]])
      : Option[ActorRef[DurableProducerQueue.Command[A]]] = {

    durableQueueBehavior.map { b =>
      val ref = context.spawn(b, "durable")
      context.watch(ref) // FIXME handle terminated, but it's supposed to be restarted so death pact is alright
      askLoadState(context, Some(ref), attempt = 1)
      ref
    }
  }

  private def askLoadState[A: ClassTag](
      context: ActorContext[InternalCommand],
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      attempt: Int): Unit = {
    implicit val loadTimeout: Timeout = 3.seconds // FIXME config
    durableQueue.foreach { ref =>
      context.ask[DurableProducerQueue.LoadState[A], DurableProducerQueue.State[A]](
        ref,
        askReplyTo => DurableProducerQueue.LoadState[A](askReplyTo)) {
        case Success(s) => LoadStateReply(s)
        case Failure(_) => LoadStateFailed(attempt) // timeout
      }
    }
  }

  private def createInitialState[A: ClassTag](hasDurableQueue: Boolean) = {
    if (hasDurableQueue) None else Some(DurableProducerQueue.State.empty[A])
  }

  private def createState[A: ClassTag](
      self: ActorRef[InternalCommand],
      producerId: String,
      send: SequencedMessage[A] => Unit,
      producer: ActorRef[RequestNext[A]],
      loadedState: DurableProducerQueue.State[A]): State[A] = {
    val unconfirmed = loadedState.unconfirmed.toVector.zipWithIndex.map {
      case (u, i) => SequencedMessage[A](producerId, u.seqNr, u.msg, i == 0, u.ack)(self)
    }
    State(
      requested = false,
      currentSeqNr = loadedState.currentSeqNr,
      confirmedSeqNr = loadedState.highestConfirmedSeqNr,
      requestedSeqNr = 1L,
      replyAfterStore = Map.empty,
      supportResend = true,
      unconfirmed = unconfirmed,
      firstSeqNr = loadedState.highestConfirmedSeqNr + 1,
      producer,
      send)
  }

  private def waitingForStart[A: ClassTag](
      context: ActorContext[InternalCommand],
      producer: Option[ActorRef[RequestNext[A]]],
      consumerController: Option[ActorRef[ConsumerController.Command[A]]],
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      initialState: Option[DurableProducerQueue.State[A]])(
      thenBecomeActive: (
          ActorRef[RequestNext[A]],
          ActorRef[ConsumerController.Command[A]],
          DurableProducerQueue.State[A]) => Behavior[InternalCommand]): Behavior[InternalCommand] = {
    Behaviors.receiveMessagePartial[InternalCommand] {
      case RegisterConsumer(c: ActorRef[ConsumerController.Command[A]] @unchecked) =>
        (producer, initialState) match {
          case (Some(p), Some(s)) => thenBecomeActive(p, c, s)
          case (_, _)             => waitingForStart(context, producer, Some(c), durableQueue, initialState)(thenBecomeActive)
        }
      case start: Start[A] @unchecked =>
        (consumerController, initialState) match {
          case (Some(c), Some(s)) => thenBecomeActive(start.producer, c, s)
          case (_, _) =>
            waitingForStart(context, Some(start.producer), consumerController, durableQueue, initialState)(
              thenBecomeActive)
        }
      case load: LoadStateReply[A] @unchecked =>
        (producer, consumerController) match {
          case (Some(p), Some(c)) => thenBecomeActive(p, c, load.state)
          case (_, _) =>
            waitingForStart(context, producer, consumerController, durableQueue, Some(load.state))(thenBecomeActive)
        }
      case LoadStateFailed(attempt) =>
        // FIXME attempt counter, and give up
        context.log.info("LoadState attempt [{}] failed, retrying.", attempt)
        // retry
        askLoadState(context, durableQueue, attempt + 1)
        Behaviors.same
    }
  }

  private def becomeActive[A: ClassTag](
      producerId: String,
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      state: State[A]): Behavior[InternalCommand] = {

    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        val msgAdapter: ActorRef[A] = ctx.messageAdapter(msg => Msg(msg))
        val requested =
          if (state.unconfirmed.isEmpty) {
            state.producer ! RequestNext(producerId, 1L, 0L, msgAdapter, ctx.self)
            true
          } else {
            ctx.log.info("Starting with [{}] unconfirmed.", state.unconfirmed.size)
            ctx.self ! ResendFirst
            false
          }
        new ProducerController[A](ctx, producerId, durableQueue, msgAdapter, timers)
          .active(state.copy(requested = requested))
      }
    }
  }

}

private class ProducerController[A: ClassTag](
    context: ActorContext[ProducerController.InternalCommand],
    producerId: String,
    durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
    msgAdapter: ActorRef[A],
    timers: TimerScheduler[ProducerController.InternalCommand]) {
  import ProducerController._
  import ProducerController.Internal._
  import ConsumerController.SequencedMessage
  import DurableProducerQueue.StoreMessageSent
  import DurableProducerQueue.StoreMessageSentAck
  import DurableProducerQueue.StoreMessageConfirmed
  import DurableProducerQueue.MessageSent
  import DurableProducerQueue.NoQualifier

  private implicit val askTimeout: Timeout = 3.seconds // FIXME config

  private def active(s: State[A]): Behavior[InternalCommand] = {

    def onMsg(m: A, newReplyAfterStore: Map[SeqNr, ActorRef[SeqNr]], ack: Boolean): Behavior[InternalCommand] = {
      checkOnMsgRequestedState()
      // FIXME adjust all logging, most should probably be debug
      context.log.info("sent [{}]", s.currentSeqNr)
      val seqMsg = SequencedMessage(producerId, s.currentSeqNr, m, s.currentSeqNr == s.firstSeqNr, ack)(context.self)
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
          s.producer ! RequestNext(producerId, s.currentSeqNr + 1, s.confirmedSeqNr, msgAdapter, context.self)
          true
        }
      active(
        s.copy(
          requested = newRequested,
          currentSeqNr = s.currentSeqNr + 1,
          replyAfterStore = newReplyAfterStore,
          unconfirmed = newUnconfirmed))
    }

    def checkOnMsgRequestedState(): Unit = {
      if (!s.requested || s.currentSeqNr > s.requestedSeqNr) {
        throw new IllegalStateException(
          s"Unexpected Msg when no demand, requested ${s.requested}, " +
          s"requestedSeqNr ${s.requestedSeqNr}, currentSeqNr ${s.currentSeqNr}")
      }
    }

    def onAck(newConfirmedSeqNr: SeqNr): State[A] = {
      val (replies, newReplyAfterStore) = s.replyAfterStore.partition { case (seqNr, _) => seqNr <= newConfirmedSeqNr }
      if (replies.nonEmpty)
        context.log.info("Confirmation replies from [{}] to [{}]", replies.head._1, replies.last._1)
      replies.foreach {
        case (seqNr, replyTo) => replyTo ! seqNr
      }

      val newUnconfirmed =
        if (s.supportResend) s.unconfirmed.dropWhile(_.seqNr <= newConfirmedSeqNr)
        else Vector.empty

      if (newConfirmedSeqNr == s.firstSeqNr)
        timers.cancel(ResendFirst)

      val newMaxConfirmedSeqNr = math.max(s.confirmedSeqNr, newConfirmedSeqNr)

      durableQueue.foreach { d =>
        // Storing the confirmedSeqNr can be "write behind", at-least-once delivery
        // FIXME to reduce number of writes we could consider to only StoreMessageConfirmed for the Request messages and not for each Ack
        if (newMaxConfirmedSeqNr != s.confirmedSeqNr)
          d ! StoreMessageConfirmed(newMaxConfirmedSeqNr, NoQualifier)
      }

      s.copy(confirmedSeqNr = newMaxConfirmedSeqNr, replyAfterStore = newReplyAfterStore, unconfirmed = newUnconfirmed)
    }

    def resendUnconfirmed(newUnconfirmed: Vector[SequencedMessage[A]]): Unit = {
      if (newUnconfirmed.nonEmpty)
        context.log.info("resending [{} - {}]", newUnconfirmed.head.seqNr, newUnconfirmed.last.seqNr)
      newUnconfirmed.foreach(s.send)
    }

    Behaviors.receiveMessage {
      case MessageWithConfirmation(m: A, replyTo) =>
        val newReplyAfterStore = s.replyAfterStore.updated(s.currentSeqNr, replyTo)
        if (durableQueue.isEmpty) {
          onMsg(m, newReplyAfterStore, ack = true)
        } else {
          storeMessageSent(MessageSent(s.currentSeqNr, m, ack = true, NoQualifier), attempt = 1)
          active(s.copy(replyAfterStore = newReplyAfterStore))
        }

      case Msg(m: A) =>
        if (durableQueue.isEmpty) {
          onMsg(m, s.replyAfterStore, ack = false)
        } else {
          storeMessageSent(MessageSent(s.currentSeqNr, m, ack = false, NoQualifier), attempt = 1)
          Behaviors.same
        }

      case StoreMessageSentCompleted(MessageSent(seqNr, m: A, ack, NoQualifier)) =>
        if (seqNr != s.currentSeqNr)
          throw new IllegalStateException(s"currentSeqNr [${s.currentSeqNr}] not matching stored seqNr [$seqNr]")

        s.replyAfterStore.get(seqNr).foreach { replyTo =>
          context.log.info("Confirmation reply to [{}] after storage", seqNr)
          replyTo ! seqNr
        }
        val newReplyAfterStore = s.replyAfterStore - seqNr

        onMsg(m, newReplyAfterStore, ack)

      case f: StoreMessageSentFailed[A] =>
        // FIXME attempt counter, and give up
        context.log.info(s"StoreMessageSent seqNr [{}] failed, attempt [{}], retrying.", f.messageSent.seqNr, f.attempt)
        // retry
        storeMessageSent(f.messageSent, attempt = f.attempt + 1)
        Behaviors.same

      case Request(newConfirmedSeqNr, newRequestedSeqNr, supportResend, viaTimeout) =>
        context.log.infoN(
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

        // when supportResend=false the requestedSeqNr window must be expanded if all sent messages were lost
        val newRequestedSeqNr2 =
          if (!supportResend && newRequestedSeqNr <= stateAfterAck.currentSeqNr)
            stateAfterAck.currentSeqNr + (newRequestedSeqNr - newConfirmedSeqNr)
          else
            newRequestedSeqNr
        if (newRequestedSeqNr2 != newRequestedSeqNr)
          context.log.infoN(
            "Expanded requestedSeqNr from [{}] to [{}], because current [{}] and all were probably lost",
            newRequestedSeqNr,
            newRequestedSeqNr2,
            stateAfterAck.currentSeqNr)

        if (newRequestedSeqNr2 > s.requestedSeqNr) {
          if (!s.requested && (newRequestedSeqNr2 - s.currentSeqNr) > 0)
            s.producer ! RequestNext(producerId, s.currentSeqNr, newConfirmedSeqNr, msgAdapter, context.self)
          active(
            stateAfterAck.copy(
              requested = true,
              requestedSeqNr = newRequestedSeqNr2,
              supportResend = supportResend,
              unconfirmed = newUnconfirmed))
        } else {
          active(stateAfterAck.copy(supportResend = supportResend, unconfirmed = newUnconfirmed))
        }

      case Ack(newConfirmedSeqNr) =>
        context.log.infoN("Ack, confirmed [{}], current [{}]", newConfirmedSeqNr, s.currentSeqNr)
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
          context.log.info("resending first, [{}]", s.firstSeqNr)
          s.send(s.unconfirmed.head.copy(first = true)(context.self))
        } else {
          if (s.currentSeqNr > s.firstSeqNr)
            timers.cancel(ResendFirst)
        }
        Behaviors.same

      case start: Start[A] =>
        context.log.info("Register new Producer [{}], currentSeqNr [{}].", start.producer, s.currentSeqNr)
        if (s.requested)
          start.producer ! RequestNext(producerId, s.currentSeqNr, s.confirmedSeqNr, msgAdapter, context.self)
        active(s.copy(producer = start.producer))

      case RegisterConsumer(consumerController: ActorRef[ConsumerController.Command[A]] @unchecked) =>
        val newFirstSeqNr =
          if (s.unconfirmed.isEmpty) s.currentSeqNr
          else s.unconfirmed.head.seqNr
        context.log.info(
          "Register new ConsumerController [{}], starting with seqNr [{}].",
          consumerController,
          newFirstSeqNr)
        if (s.unconfirmed.nonEmpty) {
          timers.startTimerWithFixedDelay(ResendFirst, ResendFirst, 1.second)
          context.self ! ResendFirst
        }
        // update the send function
        val newSend = consumerController ! _
        active(s.copy(firstSeqNr = newFirstSeqNr, send = newSend))
    }
  }

  private def storeMessageSent(messageSent: MessageSent[A], attempt: Int): Unit = {
    context.ask[StoreMessageSent[A], StoreMessageSentAck](
      durableQueue.get,
      askReplyTo => StoreMessageSent(messageSent, askReplyTo)) {
      case Success(_) => StoreMessageSentCompleted(messageSent)
      case Failure(_) => StoreMessageSentFailed(messageSent, attempt) // timeout
    }
  }
}
