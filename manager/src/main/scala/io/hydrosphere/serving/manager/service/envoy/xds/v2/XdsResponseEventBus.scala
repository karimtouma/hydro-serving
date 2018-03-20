package io.hydrosphere.serving.manager.service.envoy.xds.v2

import akka.actor.ActorRef
import akka.event.{ActorEventBus, LookupClassification}
import io.hydrosphere.serving.manager.service.envoy.xds.dto.{SubscriptionAddress, UpdateRequest, UpdateResponse}

abstract class XdsEventBus[T, I] extends ActorEventBus with LookupClassification{

  override protected def publish(event: T, subscriber: ActorRef): Unit =
    subscriber ! event

  override type Event = T
  override type Classifier = I
}

class XdsRequestEventBus extends XdsEventBus[UpdateRequest, String]{
  override protected def mapSize(): Int = 16
  override protected def classify(event: UpdateRequest): String =
    event.request.typeUrl
}

class XdsResponseEventBus extends XdsEventBus[UpdateResponse, SubscriptionAddress]{
  override protected def mapSize(): Int = 16
  override protected def classify(event: UpdateResponse): SubscriptionAddress =
    SubscriptionAddress(event.response.typeUrl, event.node)
}
