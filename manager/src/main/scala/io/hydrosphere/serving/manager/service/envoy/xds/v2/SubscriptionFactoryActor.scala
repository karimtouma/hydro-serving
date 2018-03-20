package io.hydrosphere.serving.manager.service.envoy.xds.v2

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import envoy.api.v2.{DiscoveryRequest, DiscoveryResponse, Node}
import io.hydrosphere.serving.manager.service.envoy.xds.dto.{GetNodes, StartAck, StartMessage, SubscriptionAddress}

import scala.concurrent.Future
import scala.concurrent.duration._


class SubscriptionFactoryActor(requestBus: XdsEventBus[DiscoveryRequest, String],
                               responseBus: XdsEventBus[DiscoveryResponse, SubscriptionAddress]) extends Actor with ActorLogging {

  implicit val ctx = scala.concurrent.ExecutionContext.global

  override def receive: Receive = {
    case message: StartMessage => createSubscription(message)
    case nodesRequest: GetNodes => collectWorkingNodes(nodesRequest)
    case m:Any => log.error(s"unknown message type: ${m.getClass}")
  }

  def collectWorkingNodes(request: GetNodes): Unit = {
    implicit val timeout: Timeout = 2 seconds

    val futureNodes:Iterable[Future[Option[Node]]] = context.children.map{child =>
      (child ? request).mapTo[Option[Node]].recover{case _ => None}
    }

    val nodes = Future.sequence(futureNodes).map(_.filter(_.isDefined).map(_.get).toSet)

    pipe(nodes) to sender()
  }

  def createSubscription(message: StartMessage): Unit = {
    val subscription = context.actorOf(Props(classOf[SubscriptionActor], requestBus, responseBus), "grpc-connection")
    implicit val timeout: Timeout = 2 seconds
    val startAck = subscription ? message
    pipe(startAck.mapTo[StartAck]) to sender()
  }

}
