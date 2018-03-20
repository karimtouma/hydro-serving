package io.hydrosphere.serving.manager.service.envoy.xds.v2

import akka.actor.{Actor, ActorLogging, PoisonPill}
import envoy.api.v2.{DiscoveryRequest, DiscoveryResponse, Node}
import io.grpc.stub.StreamObserver
import io.hydrosphere.serving.manager.service.envoy.xds.dto._


class SubscriptionActor(requestBus:XdsEventBus[UpdateRequest, String],
                        responseBus:XdsEventBus[UpdateResponse, SubscriptionAddress]
                       ) extends Actor with ActorLogging {

  var versions: Map[String, VersionInfo] = Map()
  var stream:StreamObserver[DiscoveryResponse] = _
  var node:Option[Node] = None

  override def receive: Receive = {
    case StartMessage(in) => start(in)
    case response:DiscoveryResponse => compareEndSend(response)
    case response:UpdateResponse => compareEndSend(response.response)
    case request:DiscoveryRequest => subscribeAndAsk(request)
    case request:UpdateRequest => subscribeAndAsk(request.request)
    case _:GetNodes => sender() ! node
    case m:Any => log.error(s"Unknown message type: $m")
  }

  def start(in:StreamObserver[DiscoveryResponse]): Unit = {
    stream = in
    log.info(s"actor $self trying to connect to stream")

    val response = new StreamObserver[DiscoveryRequest] {
      override def onError(t: Throwable): Unit = onError(t)
      override def onCompleted(): Unit = shutdown()
      override def onNext(value: DiscoveryRequest): Unit = self ! value
    }
    sender() ! StartAck(response)
  }

  def compareEndSend(response:DiscoveryResponse): Unit = {
    val version = withStateVersion(response.typeUrl, response.versionInfo)
    versions = versions + (response.typeUrl -> version)

    if(response.versionInfo > version.nodeVersion){
      stream.onNext(response)
    }
  }

  def onError(t:Throwable) = {
    log.error(s"error in nodes actor: $node", t)
    shutdown()
  }

  def shutdown() = {
    val subscriptions = versions.keySet.foldLeft(""){(f, s)=> f + s"\n$s"}
    log.info(s"stopping and unsubscribing node $node from typeUrls: $subscriptions")
    responseBus.unsubscribe(self)
    self ! PoisonPill
  }

  def subscribeAndAsk(request:DiscoveryRequest):Unit = {
    val newNode = request.node.getOrElse(Node.defaultInstance)
    if(versions.get(request.typeUrl).isEmpty){
      log.info(s"node:${request.node} subscribed to ${request.typeUrl}")

      //subscribing to unaddressed update events (events without node)
      //for broadcasting
      responseBus.subscribe(self, SubscriptionAddress(request.typeUrl))
    }
    versions = versions + (request.typeUrl -> withNodeVersion(request.typeUrl, request.versionInfo))
    if(Some(newNode) != node){
      node = Some(newNode)

      // subscription for addressed events
      responseBus.subscribe(self, SubscriptionAddress(request.typeUrl, node))
    }
    requestBus.publish(UpdateRequest(request))
  }

  def getVersion(typeUrl:String) = versions.get(typeUrl)
    .getOrElse(VersionInfo("0","0"))

  def withStateVersion(typeUrl:String, stateVersion:String) = getVersion(typeUrl).copy(stateVersion = stateVersion)

  def withNodeVersion(typeUrl:String, nodeVersion:String) = getVersion(typeUrl).copy(nodeVersion = nodeVersion)


}
