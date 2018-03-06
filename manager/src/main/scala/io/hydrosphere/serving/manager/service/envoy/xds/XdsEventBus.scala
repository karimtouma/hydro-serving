package io.hydrosphere.serving.manager.service.envoy.xds

import akka.actor.ActorRef
import akka.event.{ActorEventBus, LookupClassification}
import envoy.api.v2.DiscoveryResponse

class XdsEventBus extends ActorEventBus with LookupClassification{
  override protected def mapSize(): Int = 16

  override protected def classify(event: DiscoveryResponse): String =
    event.versionInfo

  override protected def publish(event: DiscoveryResponse, subscriber: ActorRef): Unit =
    subscriber ! event

  override type Event = DiscoveryResponse
  override type Classifier = String
}
