package io.hydrosphere.serving.manager.service

import akka.actor.ActorSystem
import io.hydrosphere.serving.manager.model.{Application, Service}
import io.hydrosphere.serving.manager.service.clouddriver.CloudService
import io.hydrosphere.serving.manager.service.envoy.xds.dto._

class InternalManagerEventsPublisher(implicit actorSystem: ActorSystem) {

  def applicationChanged(application: Application): Unit =
    actorSystem.eventStream.publish(ApplicationChanged(application))

  def applicationRemoved(application: Application): Unit =
    actorSystem.eventStream.publish(ApplicationRemoved(application))

  def serviceChanged(service: Service): Unit =
    actorSystem.eventStream.publish(ServiceChanged(service))

  def serviceRemoved(service: Service): Unit =
    actorSystem.eventStream.publish(ServiceRemoved(service))

  def cloudServiceDetected(cloudService: Seq[CloudService]): Unit =
    actorSystem.eventStream.publish(CloudServiceDetected(cloudService))
}


