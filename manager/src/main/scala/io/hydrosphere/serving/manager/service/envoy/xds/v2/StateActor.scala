package io.hydrosphere.serving.manager.service.envoy.xds.v2

import java.util.concurrent.atomic.AtomicLong

import akka.actor.Actor
import envoy.api.v2.{DiscoveryRequest, DiscoveryResponse}
import io.hydrosphere.serving.manager.service.envoy.xds.dto.{SubscriptionAddress, UpdateRequest, UpdateResponse}

abstract class StateActor[State](typeUrl:String)(implicit requestBus:XdsEventBus[UpdateRequest, String],
                 responseBus:XdsEventBus[UpdateResponse, SubscriptionAddress]
                ) extends Actor {

  val ROUTE_CONFIG_NAME = "mesh"
  val version = new AtomicLong(System.currentTimeMillis())
  private val sentRequest = new AtomicLong(1)
  var state:State = initialState()

  def initialState():State

  def resources(state:State):Seq[com.google.protobuf.any.Any]

  requestBus.subscribe(self, typeUrl)

  override def receive: Receive = {
    case request:UpdateRequest => {
      val updateResponse = UpdateResponse(createResponse(Some(request.request)), request.node)
      responseBus.publish(updateResponse)
    }
    case x =>
      if (receiveStoreChangeEvents(x)) {
        increaseVersionAndPublish()
    }
  }

  def createResponse(request:Option[DiscoveryRequest] = None) = DiscoveryResponse(
    versionInfo = version.get().toString,
    resources = resources(state),
    canary = false,
    typeUrl = typeUrl,
    nonce = sentRequest.incrementAndGet().toString
  )

  private def increaseVersionAndPublish() = {
    version.incrementAndGet()
    broadcastState()
  }

  def receiveStoreChangeEvents(mes: Any): Boolean

  def broadcastState() = {
    val notification:DiscoveryResponse = createResponse()
    responseBus.publish(UpdateResponse(notification))
  }
}
