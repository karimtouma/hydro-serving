package io.hydrosphere.serving.manager.service.envoy.xds

import akka.actor.{Actor, ActorLogging}
import akka.event.ActorEventBus
import envoy.api.v2.{DiscoveryResponse, Node}
import io.grpc.stub.StreamObserver



class SubscriptionActor(stream:StreamObserver[DiscoveryResponse],
                         node:Node
                       ) extends Actor with ActorLogging {

  var version:String = _

  override def receive: Receive = {
    case response:DiscoveryResponse => version = {



      response.versionInfo
    }
  }
}
